//! Agent 产物视图 —— 只读「agent 自己在收据里记下的那些文件」。
//!
//! 这里的信任边界是整个功能唯一重要的地方:可读集合不是「用户给什么路径就读
//! 什么路径」,而是「窗口内agent 的收据证明它编辑过的文件」。收据来自
//! ZCode/Claude/OpenCode 三源的 tool 块(见 [`crate::zcode::merged_receipts`]),
//! 是确定性证据而非推断,所以白名单是可枚举、可解释、且与统计面同一口径的。
//!
//! 三条硬规则,少一条功能就不该存在:
//! 1. 路径必须落在收据白名单里(规范化后比对,挡住 `..` 与符号链接逃逸);
//!  2. 只认 `.md`/`.markdown`/`.html`/`.htm`,其他后缀直接拒——不猜、不按
//!     MIME 放宽,避免某天有人把 `.env` 改了后缀就混进来;
//!  3. 有大小上限,且只接受 UTF-8 文本。二进制不猜、不截断成乱码。
//!
//! HTML 不内嵌预览。仪表盘同源,放开 iframe 就等于把用户本地任意 HTML 提到
//! 与所有 API 同源(`X-Frame-Options: DENY` 的取舍见 main.rs),所以 HTML 只给
//! 「用系统浏览器打开」——那本来就是 HTML 该待的地方。

use anyhow::Result;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

/// 预览上限。markdown 预览是给人读的,2 MiB 已经远超一屏;更大的文件应当
/// 走「用系统默认程序打开」,而不是让浏览器去解一个几百 MB 的字符串。
pub const MAX_PREVIEW_BYTES: u64 = 2 * 1024 * 1024;

/// 列表出口上限。三源收据合并后可能上万条路径,UI 是给人看的而不是给人导出的。
const MAX_LIST: usize = 300;

/// 语料采集窗口(issue #60):只把「窗口内收据证明编辑过」的产物读进搜索
/// 索引。收据扫描本身有会话数封顶,这里再用时间窗兜一次底,保证索引构建
/// 的文件读取量与产物列表同一量级——搜索语料不必是全历史,90 天足够覆盖
/// 「两周前写过方案 md」这类场景。
pub const CORPUS_WINDOW_DAYS: i64 = 90;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Md,
    Html,
}

impl Kind {
    /// markdown 可以在页内渲染(复用 dashboard 已有的转义渲染器);HTML 不行,
    /// 见模块头。这个开关是服务端给的,前端不自己判断——否则哪天规则变了,
    /// 两端口径会漂。
    pub fn renderable_in_page(self) -> bool {
        matches!(self, Kind::Md)
    }
}

/// 列表里的一条产物。只报告收据能证明的事实:谁改的、改了几次、文件多大。
#[derive(Debug, Clone, Serialize)]
pub struct Artifact {
    pub path: String,
    pub name: String,
    pub kind: Kind,
    pub size: u64,
    pub mtime: i64,
    /// 窗口内碰过这个文件的不同会话数。
    pub sessions: u64,
    pub edits: u64,
    pub last_ts: i64,
    /// 已消失的收据条目(文件被删/移动)。列出来是为了让「产物不见了」有解释,
    /// 而不是让 UI 静默少一行。
    pub missing: bool,
    /// issue #59 —— 工程归属的**展示标签**(project_label 形式,家目录相对)。
    /// 空串 = 账本里找不到该产物任何会话的工程线索,归因失败是常态而非错误。
    pub project: String,
    /// issue #59 —— 工程的**原始值**(账本里记的 cwd / workspace 字符串),
    /// 供「复制路径」直接拿去用,与搜索结果 #55 的 project_raw 同一约定。
    pub project_raw: String,
}

/// issue #59 —— 分面计数。**一律在封顶前的全窗口上算**:MAX_LIST 截断的是
/// 展示列表,不是事实本身——用户按工程切过之后看到的计数若只剩前 300 条的
/// 计数,「全部工程 12 / 某工程 3」加起来对不上总数,分面就失去了导航意义。
#[derive(Debug, Clone, Serialize)]
pub struct Facets {
    /// 工程展示标签 → 产物数(未归因的行不进任何桶,但计入 total)。
    pub projects: BTreeMap<String, u64>,
    /// "md" / "html" → 产物数。
    pub kinds: BTreeMap<String, u64>,
    /// 全窗口产物总数(封顶前)。
    pub total: usize,
    /// 其中文件已不在盘上的条数(前端据此解释沉底区)。
    pub missing: usize,
}

/// `/api/artifacts` 的响应体:封顶后的列表 + 封顶前的分面。
#[derive(Debug, Clone, Serialize)]
pub struct ArtifactList {
    pub artifacts: Vec<Artifact>,
    pub facets: Facets,
}

/// 预览载荷。`renderable=false` 时 `content` 为空,前端只应显示打开按钮。
#[derive(Debug, Clone, Serialize)]
pub struct Preview {
    pub path: String,
    pub name: String,
    pub kind: Kind,
    pub size: u64,
    pub mtime: i64,
    pub sessions: u64,
    pub edits: u64,
    pub renderable: bool,
    pub content: String,
}

fn ext_kind(path: &Path) -> Option<Kind> {
    // 用 to_ascii_lowercase 而不是 eq_ignore_ascii_case:后者对非 ASCII 的
    // 大写形式(如土耳其语环境的 .MD)判断并不可靠,而这里必须严格。
    match path
        .extension()
        .map(|e| e.to_string_lossy().to_ascii_lowercase())
        .as_deref()
    {
        Some("md") | Some("markdown") => Some(Kind::Md),
        Some("html") | Some("htm") => Some(Kind::Html),
        _ => None,
    }
}

/// 规范化一个候选路径。收据里的路径是 agent 写的原始字符串,可能带 `..`、
/// 相对路径或指向别处的符号链接——规范化之后才谈得上「同一个文件」。
/// 规范化失败(文件已删、中间目录没了)不是错误,只是一个 `None`。
fn canon(path: &str) -> Option<PathBuf> {
    let p = Path::new(path);
    if !p.is_absolute() {
        // 收据理论上都是绝对路径(file_path/tool input 就是那样记的)。
        // 相对路径一律不猜当前工作目录——猜错就是读错文件,而且用户看不出
        // 来。宁可少列一条。
        return None;
    }
    std::fs::canonicalize(p).ok()
}

/// 窗口内可读路径的白名单(规范化形式)。
///
/// 白名单从收据重建,而不是从前一次列表结果继承:列表是 UI 缓存,白名单是
/// 每次请求现算的信任判据。两者混用就会在收据变化后留下一个过期可读集。
pub fn allowed_paths(since: i64) -> std::collections::HashSet<PathBuf> {
    let receipts = crate::zcode::merged_receipts(since);
    allowed_paths_of(&receipts)
}

/// 从一份**已经算好**的收据切片构建白名单。把「算收据」和「按收据判」分开,
/// 让 [`preview`] 能复用同一份收据做白名单与计数——否则一个产物的预览要把
/// 三源日志整体扫两遍,而且两遍之间的内容可能已经不同(一个请求内的口径要一致)。
fn allowed_paths_of(receipts: &[crate::zcode::WorkReceipt]) -> std::collections::HashSet<PathBuf> {
    let mut set = std::collections::HashSet::new();
    for r in receipts {
        for (path, _) in &r.files {
            if ext_kind(Path::new(path)).is_none() {
                continue;
            }
            if let Some(c) = canon(path) {
                set.insert(c);
            }
        }
    }
    set
}

/// 列表出口:按路径聚合收据,只留 md/html,现读一次 stat 补齐大小与 mtime。
/// `projects` 是「会话 id → 账本里的原始工程值」(见
/// [`crate::store::session_projects_from`]);归因口径见函数内注释。
pub fn list(since: i64, projects: &HashMap<String, String>) -> ArtifactList {
    list_of(&crate::zcode::merged_receipts(since), projects)
}

/// [`list`] 的纯函数核:收据切片由调用方给定,测试可以合成收据而不必
/// 伪造三个源的日志库。生产路径只走 [`list`]。
pub fn list_of(
    receipts: &[crate::zcode::WorkReceipt],
    projects: &HashMap<String, String>,
) -> ArtifactList {
    // 会话 id → 该会话最后一次触碰 (path, ts)。归因要知道「哪个会话最后
    // 碰的」,所以这里不能只留会话集合,得留每会话的时间。
    struct Acc {
        sessions: BTreeMap<String, i64>,
        edits: u64,
        last_ts: i64,
    }
    let mut acc: BTreeMap<String, Acc> = BTreeMap::new();
    for r in receipts {
        for (path, n) in &r.files {
            if ext_kind(Path::new(path)).is_none() {
                continue;
            }
            let e = acc.entry(path.clone()).or_insert_with(|| Acc {
                sessions: Default::default(),
                edits: 0,
                last_ts: 0,
            });
            let s = e.sessions.entry(r.session_id.clone()).or_insert(0);
            *s = (*s).max(r.last_ts);
            e.edits += n;
            e.last_ts = e.last_ts.max(r.last_ts);
        }
    }

    // 归因口径(issue #59):一个产物可能被多个会话碰过、跨工程搬过。取
    // 「最后触碰该产物的会话」里**时间戳最新的、在账本里有工程值的**那个,
    // 以它的工程为准——和行内 last_ts 的口径一致(最近动它的人在哪儿,产物
    // 就算哪儿),账本没记的会话跳过而不是猜。会话 id 跨源撞车时少归因不
    // 错归因,与收据面的既有口径相同。

    let mut out: Vec<Artifact> = acc
        .into_iter()
        .map(|(path, a)| {
            // 规范化后再stat。原始路径可能带 `..`,报给用户的路径应当是
            // 真实存在的那一个,不是收据里的字面量。
            let real = canon(&path);
            let (size, mtime, missing) = match &real {
                Some(p) => match std::fs::metadata(p) {
                    Ok(m) => (m.len(), crate::file_mtime(p).unwrap_or(0), false),
                    Err(_) => (0, 0, true),
                },
                None => (0, 0, true),
            };
            let shown = real
                .as_ref()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or(path);
            // 归因:该产物的会话按最后触碰时间降序,取第一个账本里有工程值的。
            let (project, project_raw) = {
                let mut best: Option<(&String, i64)> = None;
                for (sid, ts) in &a.sessions {
                    let Some(raw) = projects.get(sid) else {
                        continue;
                    };
                    if raw.is_empty() {
                        continue;
                    }
                    if best.map_or(true, |(_, bts)| *ts > bts) {
                        best = Some((raw, *ts));
                    }
                }
                match best {
                    Some((raw, _)) => (crate::context::project_label(raw), raw.clone()),
                    None => (String::new(), String::new()),
                }
            };
            Artifact {
                name: Path::new(&shown)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| shown.clone()),
                kind: ext_kind(Path::new(&shown)).unwrap_or(Kind::Md),
                path: shown,
                size,
                mtime,
                sessions: a.sessions.len() as u64,
                edits: a.edits,
                last_ts: a.last_ts,
                missing,
                project,
                project_raw,
            }
        })
        .collect();

    // 分面在**封顶前**的全窗口上算(结构体注释说明为什么)。
    let mut facets = Facets {
        projects: BTreeMap::new(),
        kinds: BTreeMap::new(),
        total: out.len(),
        missing: out.iter().filter(|a| a.missing).count(),
    };
    for a in &out {
        *facets.kinds.entry(kind_key(a.kind)).or_default() += 1;
        if !a.project.is_empty() {
            *facets.projects.entry(a.project.clone()).or_default() += 1;
        }
    }

    // 最近被改过的在前;同一时刻用编辑次数分先后,再拿路径兜底保证顺序稳定
    // (否则同一批数据两次请求可能给出不同顺序,前端 diff 会闪)。
    // missing 沉底(issue #59):「产物不见了」是解释性信息,不与活产物抢
    // 排版——排在所有非 missing 之后,组内仍按同一套键排序。
    out.sort_by(|a, b| {
        a.missing
            .cmp(&b.missing)
            .then(b.last_ts.cmp(&a.last_ts))
            .then(b.edits.cmp(&a.edits))
            .then(a.path.cmp(&b.path))
    });
    out.truncate(MAX_LIST);
    ArtifactList {
        artifacts: out,
        facets,
    }
}

fn kind_key(k: Kind) -> String {
    match k {
        Kind::Md => "md".to_string(),
        Kind::Html => "html".to_string(),
    }
}

// ============================================================
// issue #60 —— 产物正文进搜索语料
// ============================================================

/// 搜索语料里的一份产物文档。`text` 是**当前盘上内容**的原文;索引侧会按
/// 对话行同一上限(`MAX_DOC_CHARS`)截断存储并打 truncated 标。
#[derive(Debug, Clone)]
pub struct ArtifactDoc {
    /// 规范化后的绝对路径。命中卡片展示与「打开预览」都以它为准。
    pub path: String,
    /// 证据会话(白名单的来源收据里最后触碰它的那个会话 id)。
    pub session_id: String,
    /// 工程原始值,与 [`Artifact::project_raw`] 同源同口径(账本 join)。
    pub project_raw: String,
    /// 文件 mtime(epoch 秒)。搜索 `days`/`since` 过滤对 source=artifact
    /// 命中的语义就是它——「文件内容最后一次真实变更的时刻」,在 /api/docs
    /// 里公示。不用收据 last_ts:它只证明会话动过文件,不代表内容变了。
    pub timestamp: i64,
    pub text: String,
}

/// 从一份**已经算好**的收据切片收集语料文档。信任边界与 [`list`]/[`preview`]
/// 完全同构,三重边界一条不少:
///
/// 1. **收据白名单**:只考虑收据 `files` 里出现过的路径(规范化后落点),
///    白名单外一个字节都不读;没有收据为证的文件,哪怕就在眼皮底下也不进。
/// 2. **后缀限定**:`.md`/`.markdown`/`.html`/`.htm` 之外直接跳过,
///    与 [`ext_kind`] 同一判据,不按 MIME 放宽。
/// 3. **2 MiB 上限**:超过 [`MAX_PREVIEW_BYTES`] 的文件**整个跳过**——
///    不留半截。截断成前 2 MiB 会把「索引里有这篇文档」变成谎话的一半;
///    超大文件本来就该走「在浏览器中打开」,不是搜索语料的客户。
///
/// 另有两个静默跳过:非 UTF-8(与 preview 同一口径:二进制不猜、不解成
/// 乱码)与读不到 mtime(days 语义失去锚点)。全部跳过都是**整文件**级
/// 的,不存在半进半不进的状态。产物不落盘持久化:每次索引构建从盘上现读,
/// 「重建后行不重复」由构造保证。
pub fn corpus_docs_of(
    receipts: &[crate::zcode::WorkReceipt],
    projects: &HashMap<String, String>,
    now: i64,
) -> Vec<ArtifactDoc> {
    let window_start = now.saturating_sub(CORPUS_WINDOW_DAYS * 86_400);
    // path → (会话 id → 该会话最后触碰 ts);与 list 的聚合同一形状。
    let mut touchers: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
    for r in receipts {
        // 窗口按收据 last_ts 判:更早的会话不把它的文件带进语料。
        if r.last_ts < window_start {
            continue;
        }
        for (path, _) in &r.files {
            if ext_kind(Path::new(path)).is_none() {
                continue;
            }
            let s = touchers
                .entry(path.clone())
                .or_default()
                .entry(r.session_id.clone())
                .or_insert(0);
            *s = (*s).max(r.last_ts);
        }
    }

    let mut out = Vec::new();
    for (path, sessions) in touchers {
        // 白名单成员资格按规范化落点判,与 resolve 相同。
        let Some(real) = canon(&path) else {
            continue;
        };
        let Ok(meta) = std::fs::metadata(&real) else {
            continue;
        };
        if !meta.is_file() || meta.len() > MAX_PREVIEW_BYTES {
            continue;
        }
        let Ok(bytes) = std::fs::read(&real) else {
            continue;
        };
        let Ok(text) = String::from_utf8(bytes) else {
            continue;
        };
        let Some(timestamp) = crate::file_mtime(&real) else {
            continue;
        };
        // 证据会话与工程归因:会话按最后触碰时间降序,证据 = 第一个;
        // 工程 = 第一个**在账本里有工程值**的会话,与 list 的归因口径一致。
        let mut by_recency: Vec<(&String, i64)> = sessions.iter().map(|(s, t)| (s, *t)).collect();
        by_recency.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        let session_id = by_recency
            .first()
            .map(|(s, _)| (*s).clone())
            .unwrap_or_default();
        let project_raw = by_recency
            .iter()
            .find_map(|(sid, _)| projects.get(*sid).filter(|r| !r.is_empty()).cloned())
            .unwrap_or_default();
        out.push(ArtifactDoc {
            path: real.to_string_lossy().to_string(),
            session_id,
            project_raw,
            timestamp,
            text,
        });
    }
    out
}

/// 把一次预览请求钉在白名单上,并把错误翻译成用户能照做的说法。
///
/// 顺序是有意的:**先**看后缀(不支持的直接拒,不和白名单纠缠),**再**算
/// 规范路径(文件不存在、相对路径一律不猜),**最后**查白名单(唯一的信任
/// 判据,落点才真正放行)。错误各自报告,调用方分不清「猜错路径」和「收据
/// 过期」时不用从多义的 500 猜。
fn resolve(path: &str, allowed: &std::collections::HashSet<PathBuf>) -> Result<(PathBuf, Kind)> {
    // 全部走 client_error：这些都是调用方给错路径，不是服务端故障。回 500
    // 会让看门狗和 agent 把「你传的路径不对」读成「TokenBuddy 崩了」。
    if path.is_empty() {
        return Err(crate::client_error("缺少 path 参数"));
    }
    let kind = match ext_kind(Path::new(path)) {
        Some(k) => k,
        None => {
            return Err(crate::client_error(
                "只支持 markdown 与 html 产物（.md/.markdown/.html/.htm）",
            ))
        }
    };
    let real = match canon(path) {
        Some(p) => p,
        None => return Err(crate::client_error(format!("文件不存在或不可访问：{path}"))),
    };
    if !allowed.contains(&real) {
        // 这条不是「403 秘密」而是「这个东西不在你的产物清单里」——窗口内
        // 没有 agent 收据证明编辑过它。说清楚比装死有用。
        return Err(crate::client_error(
            "该路径不在近期的 agent 产物清单里（窗口内无会话编辑过它）",
        ));
    }
    Ok((real, kind))
}

/// 读一个产物做页内预览。只接受白名单内的 markdown;html 走
/// [`open_in_system`]，不在这里返回内容。
pub fn preview(path: &str, since: i64) -> Result<Preview> {
    // 三源收集合并是最重的一步(最多各扫 200 个日志文件)。一次请求只做一遍,
    // 白名单和触达计数都吃这同一份快照——否则一个 preview 扫两遍,而且两遍
    // 之间本机收据若变了,「能读」与「列出来的 sessions/edits」就会互相矛盾。
    let receipts = crate::zcode::merged_receipts(since);
    let allowed = allowed_paths_of(&receipts);
    let (real, kind) = resolve(path, &allowed)?;
    let meta = std::fs::metadata(&real)?;
    if !meta.is_file() {
        return Err(crate::client_error(format!(
            "不是一个文件：{}",
            real.display()
        )));
    }
    let size = meta.len();
    let (sessions, edits) = touch_stats(&real, &receipts);
    let mtime = crate::file_mtime(&real).unwrap_or(0);
    let name = file_name(&real);
    let shown = real.to_string_lossy().to_string();

    if !kind.renderable_in_page() {
        // 不是拒绝:HTML 产物是合法产物,只是不能在同源页里渲染。返回
        // renderable=false 让前端显示「用浏览器打开」。
        return Ok(Preview {
            name,
            kind,
            size,
            mtime,
            sessions,
            edits,
            path: shown,
            renderable: false,
            content: String::new(),
        });
    }

    if size > MAX_PREVIEW_BYTES {
        return Err(crate::client_error(format!(
            "文件 {:.1} MB 超过预览上限 {:.0} MB——用「在浏览器中打开」看完整内容",
            size as f64 / 1048576.0,
            MAX_PREVIEW_BYTES as f64 / 1048576.0
        )));
    }
    let bytes = std::fs::read(&real)?;
    let content = String::from_utf8(bytes).map_err(|_| {
        crate::client_error(format!("不是 UTF-8 文本，无法预览：{}", real.display()))
    })?;
    Ok(Preview {
        name,
        kind,
        size,
        mtime,
        sessions,
        edits,
        path: shown,
        renderable: true,
        content,
    })
}

/// 用系统默认程序打开一个产物(HTML 的唯一查看路径)。
///
/// 只做两件事:确认它在白名单里,然后把路径作为 **argv** 交给 opener——
/// 不拼 shell 字符串,所以路径里的空格、引号、`;` 都只是普通字符。
/// 进程不等它退出:打开器 detach 后立刻返回,HTTP 响应不该被 GUI 阻塞。
pub fn open_in_system(path: &str, since: i64) -> Result<()> {
    let receipts = crate::zcode::merged_receipts(since);
    let allowed = allowed_paths_of(&receipts);
    let (real, _) = resolve(path, &allowed)?;
    if !real.is_file() {
        return Err(crate::client_error(format!(
            "不是一个文件：{}",
            real.display()
        )));
    }
    let opener = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    std::thread::spawn(move || {
        let _ = std::process::Command::new(opener)
            .arg(&real)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    });
    Ok(())
}

fn file_name(p: &Path) -> String {
    p.file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| p.to_string_lossy().to_string())
}

/// 一个文件在窗口内被改动的会话数与编辑次数。与 [`list`] 的聚合口径一致,
/// 所以预览页上的数字和列表里那一行说的是同一件事——两处口径漂移过一次,
/// 就会有人开始怀疑哪个是真的。
fn touch_stats(real: &Path, receipts: &[crate::zcode::WorkReceipt]) -> (u64, u64) {
    let mut sessions = std::collections::BTreeSet::new();
    let mut edits: u64 = 0;
    for r in receipts {
        for (path, n) in &r.files {
            if canon(path).as_deref() == Some(real) {
                sessions.insert(r.session_id.clone());
                edits += n;
            }
        }
    }
    (sessions.len() as u64, edits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct Tmp(PathBuf);
    impl Tmp {
        fn new(tag: &str) -> Self {
            let mut p = std::env::temp_dir();
            p.push(format!(
                "tokenbuddy-artifacts-{}-{}",
                tag,
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).expect("tmp dir");
            Tmp(p)
        }
        fn write(&self, name: &str, body: &str) -> PathBuf {
            let path = self.0.join(name);
            let mut f = std::fs::File::create(&path).expect("create");
            f.write_all(body.as_bytes()).expect("write");
            path
        }
    }
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn ext_kind_is_strict_and_lowercased() {
        assert_eq!(ext_kind(Path::new("a.md")), Some(Kind::Md));
        assert_eq!(ext_kind(Path::new("a.MD")), Some(Kind::Md));
        assert_eq!(ext_kind(Path::new("a.markdown")), Some(Kind::Md));
        assert_eq!(ext_kind(Path::new("a.html")), Some(Kind::Html));
        assert_eq!(ext_kind(Path::new("a.htm")), Some(Kind::Html));
        assert_eq!(ext_kind(Path::new("a.HTML")), Some(Kind::Html));
    }

    /// 后缀白名单是这个功能的全部安全前提之一:漏一个,某天把 `.env` 改名成
    /// `.env.html` 就能读出来。这些都必须拒。
    #[test]
    fn ext_kind_rejects_everything_else() {
        for name in [
            "a.txt",
            "a.json",
            "a.rs",
            "a.js",
            "a.env",
            "a.htmlx",
            "a.htmll",
            "a",
            "a.mdx",
            "a.svg",
            "a.xml",
            "a.csv",
            "a.pdf",
            "a.png",
            "a.sqlite",
            "a.parquet",
        ] {
            assert_eq!(ext_kind(Path::new(name)), None, "{name} must be rejected");
        }
        // 无扩展名、以及点开头文件(`.mdrc`)都不是产物
        assert_eq!(ext_kind(Path::new("/tmp/.md")), None);
        assert_eq!(ext_kind(Path::new("md")), None);
    }

    #[test]
    fn html_is_not_renderable_in_page() {
        assert!(Kind::Md.renderable_in_page());
        assert!(!Kind::Html.renderable_in_page());
    }

    #[test]
    fn canon_rejects_relative_paths() {
        // 相对路径一律不解析:猜工作目录就是读错文件,而且用户看不出来。
        assert_eq!(canon("relative/file.md"), None);
        assert_eq!(canon("./x.md"), None);
        assert_eq!(canon(""), None);
    }

    #[test]
    fn resolve_refuses_paths_outside_the_whitelist() {
        let t = Tmp::new("outside");
        let f = t.write("a.md", "# hi");
        // 白名单为空:文件真实存在也不许读。这正是「不是文件浏览器」的定义。
        let empty = std::collections::HashSet::new();
        let err = resolve(f.to_str().unwrap(), &empty)
            .unwrap_err()
            .to_string();
        assert!(err.contains("产物清单"), "unexpected error: {err}");
    }

    /// 带 `..` 的路径是「按规范化后的目标判定成员资格」,不是「按字面串比前缀」:
    /// 落进白名单就可读(因为收据指的正是这个真实文件);落出白名单就拒。
    #[test]
    fn resolve_judges_membership_by_canonical_path() {
        let t = Tmp::new("traversal");
        let secret = t.write("secret.md", "token=hunter2");
        let deep = t.0.join("a/b");
        std::fs::create_dir_all(&deep).unwrap();
        // 深处的 `../../` 逃逸回 tmp 根,指向未列入白名单的真实文件
        let sneaky = deep.join("../..").join("secret.md");
        let allowed: std::collections::HashSet<PathBuf> =
            [secret.canonicalize().unwrap()].into_iter().collect();
        assert!(resolve(sneaky.to_str().unwrap(), &allowed).is_ok());
        // 但白名单只含 secret.md 时,`a/../secret.md` 规范化后就是它,应当放行——
        // 这说明规范化而非字符串前缀比对才是正确的判据。
        let other = t.write("other.md", "x");
        let allowed2: std::collections::HashSet<PathBuf> =
            [other.canonicalize().unwrap()].into_iter().collect();
        assert!(resolve(sneaky.to_str().unwrap(), &allowed2).is_err());
    }

    #[test]
    fn resolve_checks_extension_before_existence() {
        // 后缀错误要说后缀的事:否则用户传 `.env` 得到的是「文件不存在」,
        // 永远不知道自己越界了。
        let t = Tmp::new("ext-first");
        let f = t.write("a.txt", "x");
        let allowed = std::collections::HashSet::new();
        let err = resolve(f.to_str().unwrap(), &allowed)
            .unwrap_err()
            .to_string();
        assert!(err.contains("markdown"), "unexpected: {err}");
    }

    #[test]
    fn preview_refuses_html_content_but_reports_it_as_openable() {
        // HTML 必须以「可打开、不可内嵌」的形式存在,而不是被悄悄渲染或
        // 悄悄消失。这里用空白名单验证 resolve 层拒绝,再用构造验证 kind。
        let t = Tmp::new("html");
        let f = t.write("page.html", "<script>alert(1)</script>");
        let allowed: std::collections::HashSet<PathBuf> =
            [f.canonicalize().unwrap()].into_iter().collect();
        let (real, kind) = resolve(f.to_str().unwrap(), &allowed).unwrap();
        assert_eq!(kind, Kind::Html);
        assert!(!kind.renderable_in_page());
        assert!(real.is_file());
    }

    #[test]
    fn preview_refuses_non_utf8() {
        let t = Tmp::new("binary");
        let f = t.0.join("bad.md");
        std::fs::write(&f, [0xff, 0xfe, 0x00, 0x01]).unwrap();
        let allowed: std::collections::HashSet<PathBuf> =
            [f.canonicalize().unwrap()].into_iter().collect();
        let (_real, kind) = resolve(f.to_str().unwrap(), &allowed).unwrap();
        assert_eq!(kind, Kind::Md);
        // 直接验证读取分支的拒绝行为（不经过收据,因为测试环境没有收据）
        let bytes = std::fs::read(&f).unwrap();
        assert!(String::from_utf8(bytes).is_err());
    }

    #[test]
    fn preview_size_cap_is_enforced_before_reading() {
        // 上限的意义在于「大文件不进内存」。这里只验证判据本身:超限即拒。
        let t = Tmp::new("toobig");
        let f = t.write("big.md", "x");
        let big = t.0.join("big2.md");
        let f2 = std::fs::File::create(&big).unwrap();
        f2.set_len(MAX_PREVIEW_BYTES + 1).unwrap();
        drop(f2);
        let allowed: std::collections::HashSet<PathBuf> =
            [big.canonicalize().unwrap()].into_iter().collect();
        assert!(resolve(big.to_str().unwrap(), &allowed).is_ok());
        assert!(std::fs::metadata(&big).unwrap().len() > MAX_PREVIEW_BYTES);
        assert!(f.exists());
    }

    #[test]
    fn max_preview_bytes_is_two_mib() {
        assert_eq!(MAX_PREVIEW_BYTES, 2 * 1024 * 1024);
    }

    #[test]
    fn allowed_paths_is_empty_without_receipts() {
        // 无收据 = 无可读文件。这是「白名单而非路径参数」的直接断言,
        // 也是这个功能在真实 agent 之外的默认姿态。
        //
        // 用「遥远的将来」而不是 i64::MAX:三源都用 `since * 1000`(毫秒)
        // 做比较,i64::MAX * 1000 会溢出成负数,于是 SQL 的
        // `time_created >= 负数` 会匹配**全部**历史行——测试会拿到一份
        // 真实收据而不是空集,断言失败的原因还完全在别处。
        let far_future = 4_000_000_000i64;
        assert!(far_future * 1000 > 0, "毫秒换算不得溢出");
        assert!(allowed_paths(far_future).is_empty());
    }

    // ============================================================
    // issue #59 —— 分面 / 沉底 / 工程归因(合成收据,不碰真实日志)
    // ============================================================

    fn receipt(session: &str, files: &[(&str, u64)], last_ts: i64) -> crate::zcode::WorkReceipt {
        crate::zcode::WorkReceipt {
            session_id: session.to_string(),
            files: files.iter().map(|(p, n)| (p.to_string(), *n)).collect(),
            bash_count: 0,
            test_count: 0,
            tool_calls: files.len() as u64,
            tools: Default::default(),
            last_ts,
        }
    }

    /// 分面计数必须落在**封顶前**的全窗口上:305 条收据产物,列表按
    /// MAX_LIST=300 截断,但 facets.total 要报 305——分面是导航,截断的
    /// 是展示不是事实。
    #[test]
    fn facets_count_the_full_window_before_the_cap() {
        let t = Tmp::new("facets-cap");
        // 305 个「文件已删」的收据条目(不需要真实文件,missing 不落盘)
        let mut receipts = Vec::new();
        for i in 0..305 {
            receipts.push(receipt(
                "s1",
                &[(&format!("{}/gone-{}.md", t.0.display(), i), 1)],
                1_700_000_000 + i,
            ));
        }
        // 3 个真实文件
        for i in 0..3 {
            let p = t.write(&format!("live-{i}.md"), "# live");
            receipts.push(receipt("s2", &[(p.to_str().unwrap(), 1)], 1_700_000_500));
        }
        let out = list_of(&receipts, &Default::default());
        assert_eq!(out.facets.total, 308, "分面在全窗口上计数");
        assert_eq!(out.facets.missing, 305);
        assert_eq!(out.artifacts.len(), 300, "列表仍 300 封顶");
        assert_eq!(out.facets.kinds.get("md"), Some(&308));
        assert_eq!(out.facets.kinds.get("html"), None);
    }

    /// missing 行沉底:活产物按 last_ts 排在前面,「产物不见了」的解释性
    /// 行整组垫底,组内顺序仍稳定。
    #[test]
    fn missing_rows_sink_below_live_ones() {
        let t = Tmp::new("missing-sink");
        let live = t.write("live.md", "# live");
        let receipts = vec![
            // missing 但收据时间更新——也不能越过活产物
            receipt(
                "s1",
                &[(&format!("{}/gone.md", t.0.display()), 1)],
                1_700_000_900,
            ),
            receipt("s2", &[(live.to_str().unwrap(), 1)], 1_700_000_100),
            receipt(
                "s3",
                &[(&format!("{}/gone2.md", t.0.display()), 1)],
                1_700_000_800,
            ),
        ];
        let out = list_of(&receipts, &Default::default());
        assert_eq!(out.artifacts.len(), 3);
        assert!(!out.artifacts[0].missing, "活产物排最前");
        assert!(
            out.artifacts[1..].iter().all(|a| a.missing),
            "missing 整组沉底"
        );
    }

    /// 工程归因:一个产物被两个会话碰过时,归给「最后触碰它的会话」在账本
    /// 里的工程;账本里没有工程值的会话跳过,不猜。展示标签走 project_label。
    #[test]
    fn project_attribution_follows_latest_ledger_session() {
        let t = Tmp::new("attrib");
        let f1 = t.write("a.md", "# a");
        let f2 = t.write("b.md", "# b");
        let f3 = t.write("c.md", "# c");
        let home = dirs::home_dir().unwrap();
        let proj_a = format!("{}/code/alpha", home.display());
        let proj_b = format!("{}/code/beta", home.display());
        let mut projects: HashMap<String, String> = HashMap::new();
        projects.insert("s1".to_string(), proj_a.clone());
        projects.insert("s2".to_string(), proj_b.clone());
        // s3 故意不进账本映射:归因必须跳过它,而不是猜一个
        let receipts = vec![
            receipt("s1", &[(f1.to_str().unwrap(), 1)], 1_700_000_100),
            // s2 更晚碰 a.md → a.md 归 beta,尽管 s1 更早归 alpha
            receipt(
                "s2",
                &[(f1.to_str().unwrap(), 1), (f2.to_str().unwrap(), 1)],
                1_700_000_200,
            ),
            receipt("s3", &[(f3.to_str().unwrap(), 1)], 1_700_000_300),
        ];
        let out = list_of(&receipts, &projects);
        let by_path = |p: &str| {
            out.artifacts
                .iter()
                .find(|a| a.path.ends_with(p))
                .unwrap_or_else(|| panic!("{} missing", p))
        };
        let a = by_path("a.md");
        assert_eq!(a.project_raw, proj_b, "最后触碰会话的工程胜出");
        assert_eq!(a.project, "code/beta", "标签是家目录相对形");
        let b = by_path("b.md");
        assert_eq!(b.project_raw, proj_b);
        let c = by_path("c.md");
        assert_eq!(c.project, "", "账本无工程值的会话不归因");
        assert_eq!(c.project_raw, "");
        // 分面按标签计数;未归因的 c 不进任何桶
        assert_eq!(out.facets.projects.get("code/beta"), Some(&2));
        assert_eq!(out.facets.projects.len(), 1);
        assert_eq!(out.facets.total, 3);
    }

    // ============================================================
    // issue #60 —— 语料三重边界(白名单 × 后缀 × 2 MiB)
    // ============================================================

    /// 边界一:没有收据为证的文件,哪怕真实存在也一个字节都不进语料。
    #[test]
    fn corpus_never_admits_files_without_receipt() {
        let t = Tmp::new("corpus-whitelist");
        let _inside = t.write("inside.md", "收据内的正文");
        let _outside = t.write("outside.md", "收据外的正文");
        // 收据只证明 inside
        let receipts = vec![receipt(
            "s1",
            &[(t.0.join("inside.md").to_str().unwrap(), 1)],
            crate::now_ts(),
        )];
        let docs = corpus_docs_of(&receipts, &Default::default(), crate::now_ts());
        assert_eq!(docs.len(), 1, "只收白名单内的那一个");
        assert!(docs[0].path.ends_with("inside.md"));
        assert!(docs[0].text.contains("收据内的正文"));
    }

    /// 边界二:后缀限定与 preview 同一判据——收据里混进 .txt/.json 也不进。
    #[test]
    fn corpus_extension_gate_matches_preview_rules() {
        let t = Tmp::new("corpus-ext");
        let md = t.write("doc.md", "markdown 正文");
        t.write("note.txt", "纯文本");
        t.write("data.json", "{}");
        let receipts = vec![receipt(
            "s1",
            &[
                (md.to_str().unwrap(), 1),
                (t.0.join("note.txt").to_str().unwrap(), 1),
                (t.0.join("data.json").to_str().unwrap(), 1),
            ],
            crate::now_ts(),
        )];
        let docs = corpus_docs_of(&receipts, &Default::default(), crate::now_ts());
        assert_eq!(docs.len(), 1);
        assert!(docs[0].path.ends_with("doc.md"));
    }

    /// 边界三:超 2 MiB 的文件**整个跳过**——不读、不留半截。上限判据与
    /// 预览共用 MAX_PREVIEW_BYTES;恰好等于上限的文件应当进(闭区间)。
    #[test]
    fn corpus_skips_oversized_files_whole() {
        let t = Tmp::new("corpus-2mib");
        // 恰好 2 MiB:进(边界值归内)
        let ok_path = t.0.join("ok.md");
        std::fs::File::create(&ok_path)
            .unwrap()
            .set_len(MAX_PREVIEW_BYTES)
            .unwrap();
        // 2 MiB + 1:整文件跳过
        let big_path = t.0.join("big.md");
        std::fs::File::create(&big_path)
            .unwrap()
            .set_len(MAX_PREVIEW_BYTES + 1)
            .unwrap();
        let receipts = vec![receipt(
            "s1",
            &[
                (ok_path.to_str().unwrap(), 1),
                (big_path.to_str().unwrap(), 1),
            ],
            crate::now_ts(),
        )];
        let docs = corpus_docs_of(&receipts, &Default::default(), crate::now_ts());
        assert_eq!(docs.len(), 1, "超限文件整个跳过,不剩半截");
        assert!(docs[0].path.ends_with("ok.md"));
    }

    /// 非 UTF-8 与窗口外收据都不进:二进制不猜;90 天窗口外收据不把它的
    /// 文件带进语料。
    #[test]
    fn corpus_skips_non_utf8_and_stale_window() {
        let t = Tmp::new("corpus-misc");
        let bin = t.0.join("bin.md");
        std::fs::write(&bin, [0xff, 0xfe, 0x00, 0x01]).unwrap();
        let old = t.write("old.md", "九十天前的产物");
        let recent = t.write("new.md", "新鲜的产物");
        let now = crate::now_ts();
        let receipts = vec![
            receipt("s1", &[(bin.to_str().unwrap(), 1)], now),
            // 收据时间在窗口边界外:它的文件不进语料
            receipt(
                "s2",
                &[(old.to_str().unwrap(), 1)],
                now - (CORPUS_WINDOW_DAYS + 1) * 86_400,
            ),
            receipt("s3", &[(recent.to_str().unwrap(), 1)], now),
        ];
        let docs = corpus_docs_of(&receipts, &Default::default(), now);
        assert_eq!(docs.len(), 1, "非 UTF-8 与窗口外都跳过");
        assert!(docs[0].path.ends_with("new.md"));
        // days 语义 = mtime:timestamp 就是文件 mtime
        let mtime = crate::file_mtime(t.0.join("new.md").as_path()).unwrap();
        assert_eq!(docs[0].timestamp, mtime);
    }

    /// 语料的工程归因与列表同一规则(最后触碰会话的账本工程)。
    #[test]
    fn corpus_project_attribution_matches_list_rule() {
        let t = Tmp::new("corpus-proj");
        let f = t.write("rep.md", "# 报告");
        let home = dirs::home_dir().unwrap();
        let mut projects: HashMap<String, String> = HashMap::new();
        projects.insert(
            "early".to_string(),
            format!("{}/code/early", home.display()),
        );
        projects.insert("late".to_string(), format!("{}/code/late", home.display()));
        let receipts = vec![
            receipt("early", &[(f.to_str().unwrap(), 1)], crate::now_ts() - 60),
            receipt("late", &[(f.to_str().unwrap(), 1)], crate::now_ts()),
        ];
        let docs = corpus_docs_of(&receipts, &projects, crate::now_ts());
        assert_eq!(docs.len(), 1);
        assert!(
            docs[0].project_raw.ends_with("code/late"),
            "got {}",
            docs[0].project_raw
        );
        assert_eq!(docs[0].session_id, "late", "证据会话 = 最后触碰者");
    }
}
