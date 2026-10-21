//! #63 i18n 回归门禁(纯静态审计,零外部依赖、零网络、不碰文件系统)。
//!
//! 机制回顾:dashboard.html 的 EN 模式靠 `translateOne()` 对**文本节点**做三级匹配
//! —— I18N 整句词典 → I18N_PATTERNS 锚定正则 → I18N_PREFIX 前缀表;
//! `data-no-i18n` 子树整棵跳过;MutationObserver 让 JS 动态渲染的串走同一套词典。
//! 所以「EN 模式漏翻」= 一段会出现在 DOM 里的中文,三级都够不着。
//!
//! 本门禁从 dashboard.html 源码文本抽取三级词典,然后分别审:
//!   A. 静态 HTML 文本节点(跳过 <script>/<style>/注释与 data-no-i18n 子树);
//!   B. JS 字符串/模板字面量(跳过注释与词典区自身;模板 `${...}` 挖洞成骨架,
//!      标签边界切出近似文本节点块,HTML 属性取引号内候选,探针代入后过正则)。
//!
//! 近似性说明(已知盲区;发现新形态时扩展引擎,不许删断言或放宽 allowlist):
//!   - `${...}` 洞里嵌套的模板字面量不可见(外层洞被整体挖掉);
//!   - 未加引号的对象键(如 `{ 单发: ... }`)不可见;
//!   - 由多个字面量拼接而成的运行时节点,按「骨架探针 / 显式 allowlist」处理,
//!     allowlist 逐条注明理由,只进不出。

// ---------------------------------------------------------------- JS 字符串

/// JS 字符串字面量解码(\n \t \r \" \' \\ \/ \uXXXX;未知转义取字面字符)。
/// `chars[i]` 必须落在引号上;返回 (内容, 结束引号之后的位置)。
fn unescape_js(chars: &[char], i: usize) -> (String, usize) {
    let quote = chars[i];
    let mut out = String::new();
    let mut i = i + 1;
    while i < chars.len() {
        let c = chars[i];
        if c == '\\' {
            let e = chars.get(i + 1).copied().unwrap_or('\\');
            match e {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                'b' => out.push('\u{8}'),
                'f' => out.push('\u{c}'),
                'u' => {
                    let hex: String = chars[i + 2..(i + 6).min(chars.len())].iter().collect();
                    if let Some(ch) = u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                        out.push(ch);
                        i += 6;
                        continue;
                    }
                    out.push('u');
                }
                other => out.push(other),
            }
            i += 2;
            continue;
        }
        if c == quote {
            return (out, i + 1);
        }
        out.push(c);
        i += 1;
    }
    (out, i)
}

/// 跳过 // 与 /* */ 注释。
fn skip_comments(chars: &[char], mut i: usize) -> usize {
    loop {
        if chars.get(i) == Some(&'/') && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if chars.get(i) == Some(&'/') && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i = (i + 2).min(chars.len());
        } else {
            return i;
        }
    }
}

fn skip_ws_and_comments(chars: &[char], mut i: usize) -> usize {
    loop {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        let j = skip_comments(chars, i);
        if j == i {
            return i;
        }
        i = j;
    }
}

// ---------------------------------------------------------------- 词典抽取

/// 从 `const NAME = {` 起做括号配对(字符串感知),返回对象体。
fn extract_object_body(chars: &[char], name: &str) -> String {
    let needle: Vec<char> = format!("const {name} = {{").chars().collect();
    let start = chars
        .windows(needle.len())
        .position(|w| w == needle.as_slice())
        .unwrap_or_else(|| panic!("i18n 门禁:找不到 const {name}"));
    let mut i = start + needle.len();
    let mut depth = 1usize;
    let mut in_quote: Option<char> = None;
    let body_start = i;
    while i < chars.len() && depth > 0 {
        let c = chars[i];
        match in_quote {
            Some(q) => {
                if c == '\\' {
                    i += 2;
                    continue;
                }
                if c == q {
                    in_quote = None;
                }
            }
            None => match c {
                '"' | '\'' | '`' => in_quote = Some(c),
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            },
        }
        i += 1;
    }
    chars[body_start..i - 1].iter().collect()
}

/// 解析对象体里的 "键":"值" 对(注释已由调用方容忍)。
fn parse_object_pairs(body: &str) -> Vec<(String, String)> {
    let chars: Vec<char> = body.chars().collect();
    let mut pairs = Vec::new();
    let mut i = 0usize;
    i = skip_ws_and_comments(&chars, i);
    while i < chars.len() {
        if chars[i] != '"' && chars[i] != '\'' {
            i += 1;
            continue;
        }
        let (key, next) = unescape_js(&chars, i);
        i = skip_ws_and_comments(&chars, next);
        if chars.get(i) != Some(&':') {
            continue;
        }
        i = skip_ws_and_comments(&chars, i + 1);
        if chars.get(i) != Some(&'"') && chars.get(i) != Some(&'\'') {
            continue;
        }
        let (value, next) = unescape_js(&chars, i);
        pairs.push((key, value));
        i = skip_ws_and_comments(&chars, next);
    }
    pairs
}

// ---------------------------------------------------------------- 迷你正则
//
// 只实现 I18N_PATTERNS 用到的子集:锚点 ^ $、.、\d \D \w \W \s \S、字符类
// [...] (含区间与转义)、分组 (...) / (?:...)、择一 |、量词 * + ? {m} {m,}
// {m,n} 及惰性后缀、标点转义;`.` 不匹配 \n(与 JS 一致)。
// 遇到不支持语法 → panic(门禁失败即提示扩展引擎),绝不静默放过。

#[derive(Debug, Clone)]
enum Node {
    Start,
    End,
    Char(char),
    AnyChar,
    Class {
        neg: bool,
        items: Vec<ClassItem>,
    },
    Group(Vec<Vec<Node>>),
    Repeat {
        node: Box<Node>,
        min: usize,
        max: usize,
        lazy: bool,
    },
}

#[derive(Debug, Clone)]
enum ClassItem {
    Ch(char),
    Range(char, char),
    Digit(bool),
    Word(bool),
    Space(bool),
}

struct Regex {
    seq: Vec<Node>,
}

struct RegexParser {
    chars: Vec<char>,
    pos: usize,
}

impl RegexParser {
    fn new(body: &str) -> Self {
        Self {
            chars: body.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn parse_alternation(&mut self) -> Vec<Vec<Node>> {
        let mut alts = vec![self.parse_sequence()];
        while self.eat('|') {
            alts.push(self.parse_sequence());
        }
        alts
    }

    fn parse_sequence(&mut self) -> Vec<Node> {
        let mut seq = Vec::new();
        while let Some(c) = self.peek() {
            if c == '|' || c == ')' {
                break;
            }
            let atom = self.parse_atom();
            let atom = self.parse_quantifier(atom);
            seq.push(atom);
        }
        seq
    }

    fn parse_atom(&mut self) -> Node {
        match self.bump().unwrap() {
            '^' => Node::Start,
            '$' => Node::End,
            '.' => Node::AnyChar,
            '(' => {
                if self.peek() == Some('?') {
                    self.bump();
                    match self.bump() {
                        Some(':') => {}
                        other => panic!("i18n 门禁:正则分组语法不支持 (?{other:?}"),
                    }
                }
                let alts = self.parse_alternation();
                assert!(self.eat(')'), "i18n 门禁:正则括号不闭合");
                Node::Group(alts)
            }
            '[' => self.parse_class(),
            '\\' => self.parse_escape_class_or_char(),
            '*' | '+' | '?' => panic!("i18n 门禁:量词出现在原子位(解析器 bug)"),
            c => Node::Char(c),
        }
    }

    fn parse_escape_class_or_char(&mut self) -> Node {
        match self.bump().unwrap() {
            'd' => Node::Class {
                neg: false,
                items: vec![ClassItem::Digit(false)],
            },
            'D' => Node::Class {
                neg: false,
                items: vec![ClassItem::Digit(true)],
            },
            'w' => Node::Class {
                neg: false,
                items: vec![ClassItem::Word(false)],
            },
            'W' => Node::Class {
                neg: false,
                items: vec![ClassItem::Word(true)],
            },
            's' => Node::Class {
                neg: false,
                items: vec![ClassItem::Space(false)],
            },
            'S' => Node::Class {
                neg: false,
                items: vec![ClassItem::Space(true)],
            },
            'n' => Node::Char('\n'),
            't' => Node::Char('\t'),
            'r' => Node::Char('\r'),
            'b' => panic!("i18n 门禁:\\b 词边界不支持"),
            c => Node::Char(c),
        }
    }

    fn parse_class(&mut self) -> Node {
        let neg = self.eat('^');
        let mut items = Vec::new();
        let mut first = true;
        loop {
            let c = self
                .bump()
                .unwrap_or_else(|| panic!("i18n 门禁:字符类不闭合"));
            if c == ']' && !first {
                break;
            }
            first = false;
            let lo = if c == '\\' {
                match self.bump().unwrap() {
                    'd' => {
                        items.push(ClassItem::Digit(false));
                        continue;
                    }
                    'D' => {
                        items.push(ClassItem::Digit(true));
                        continue;
                    }
                    'w' => {
                        items.push(ClassItem::Word(false));
                        continue;
                    }
                    'W' => {
                        items.push(ClassItem::Word(true));
                        continue;
                    }
                    's' => {
                        items.push(ClassItem::Space(false));
                        continue;
                    }
                    'S' => {
                        items.push(ClassItem::Space(true));
                        continue;
                    }
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    other => other,
                }
            } else {
                c
            };
            // 区间 lo-hi(hi 不是 ']' 时;尾随 '-' 是字面字符)
            if self.peek() == Some('-')
                && self.chars.get(self.pos + 1).copied().unwrap_or(']') != ']'
            {
                self.bump(); // '-'
                let mut hi = self.bump().unwrap();
                if hi == '\\' {
                    hi = match self.bump().unwrap() {
                        'n' => '\n',
                        't' => '\t',
                        other => other,
                    };
                }
                items.push(ClassItem::Range(lo, hi));
            } else {
                items.push(ClassItem::Ch(lo));
            }
        }
        Node::Class { neg, items }
    }

    fn parse_quantifier(&mut self, atom: Node) -> Node {
        let (min, max) = match self.peek() {
            Some('*') => {
                self.bump();
                (0, usize::MAX)
            }
            Some('+') => {
                self.bump();
                (1, usize::MAX)
            }
            Some('?') => {
                self.bump();
                (0, 1)
            }
            Some('{') => {
                let save = self.pos;
                self.bump();
                let mut m = String::new();
                while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    m.push(self.bump().unwrap());
                }
                if m.is_empty() {
                    // 不是 {m,n},是字面 '{'
                    self.pos = save;
                    return atom;
                }
                let lo: usize = m.parse().unwrap();
                let hi = if self.eat(',') {
                    let mut n = String::new();
                    while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                        n.push(self.bump().unwrap());
                    }
                    if n.is_empty() {
                        usize::MAX
                    } else {
                        n.parse().unwrap()
                    }
                } else {
                    lo
                };
                assert!(self.eat('}'), "i18n 门禁:{{m,n}} 不闭合");
                (lo, hi)
            }
            _ => return atom,
        };
        let lazy = self.eat('?');
        Node::Repeat {
            node: Box::new(atom),
            min,
            max,
            lazy,
        }
    }
}

fn class_matches(items: &[ClassItem], c: char) -> bool {
    items.iter().any(|it| match it {
        ClassItem::Ch(x) => c == *x,
        ClassItem::Range(lo, hi) => *lo <= c && c <= *hi,
        ClassItem::Digit(neg) => c.is_ascii_digit() != *neg,
        // \w 在 JS 里 = [A-Za-z0-9_]
        ClassItem::Word(neg) => (c.is_ascii_alphanumeric() || c == '_') != *neg,
        ClassItem::Space(neg) => c.is_whitespace() != *neg,
    })
}

fn match_node(node: &Node, s: &[char], pos: usize, k: &dyn Fn(usize) -> bool) -> bool {
    match node {
        Node::Start => pos == 0 && k(pos),
        Node::End => pos == s.len() && k(pos),
        Node::Char(c) => pos < s.len() && s[pos] == *c && k(pos + 1),
        Node::AnyChar => pos < s.len() && s[pos] != '\n' && k(pos + 1),
        Node::Class { neg, items } => {
            pos < s.len() && class_matches(items, s[pos]) != *neg && k(pos + 1)
        }
        Node::Group(alts) => alts.iter().any(|alt| match_seq(alt, s, pos, k)),
        Node::Repeat {
            node,
            min,
            max,
            lazy,
        } => match_repeat(node, *min, *max, *lazy, 0, s, pos, k),
    }
}

fn match_seq(seq: &[Node], s: &[char], pos: usize, k: &dyn Fn(usize) -> bool) -> bool {
    match seq.split_first() {
        None => k(pos),
        Some((first, rest)) => match_node(first, s, pos, &|p| match_seq(rest, s, p, k)),
    }
}

#[allow(clippy::too_many_arguments)] // 迷你引擎的 match 续延风格:node+边界+续延 k,拆结构体更难读
fn match_repeat(
    node: &Node,
    min: usize,
    max: usize,
    lazy: bool,
    count: usize,
    s: &[char],
    pos: usize,
    k: &dyn Fn(usize) -> bool,
) -> bool {
    // 零宽推进直接拒绝,防死循环;空匹配由 min 分支的 k(pos) 兜住
    let more = |k2: &dyn Fn(usize) -> bool| {
        count < max
            && match_node(node, s, pos, &|p| {
                p != pos && match_repeat(node, min, max, lazy, count + 1, s, p, k2)
            })
    };
    if lazy {
        if count >= min && k(pos) {
            return true;
        }
        more(k)
    } else {
        if more(k) {
            return true;
        }
        count >= min && k(pos)
    }
}

impl Regex {
    /// JS 正则字面量 → 引擎。要求锚定(^...$):translateOne 的 pattern
    /// 只在整节点 fullmatch 时认领,门禁沿用同一契约。
    fn new(body: &str) -> Regex {
        let mut p = RegexParser::new(body);
        let alts = p.parse_alternation();
        assert_eq!(p.pos, p.chars.len(), "i18n 门禁:正则有未消费尾段:{body}");
        let seq = if alts.len() == 1 {
            alts.into_iter().next().unwrap()
        } else {
            vec![Node::Group(alts)]
        };
        assert!(
            matches!(seq.first(), Some(Node::Start)),
            "i18n 门禁:pattern 必须以 ^ 锚定:{body}"
        );
        assert!(
            matches!(seq.last(), Some(Node::End)),
            "i18n 门禁:pattern 必须以 $ 锚定:{body}"
        );
        Regex { seq }
    }

    fn full_match(&self, text: &str) -> bool {
        let chars: Vec<char> = text.chars().collect();
        match_seq(&self.seq, &chars, 0, &|p| p == chars.len())
    }
}

/// 从 `const I18N_PATTERNS = [` 起按项解析,只取每项 `[ /regex/, fn ]` 里的
/// 正则体。逐项跳过替换函数:模式栈式 JS 扫描(代码/单引号/双引号/模板串),
/// 模板串里的 `${...}` 视为代码重新入栈——函数体里的嵌套模板才不会乱套。
fn extract_pattern_bodies(chars: &[char], array_items_start: usize) -> Vec<String> {
    let mut bodies: Vec<String> = Vec::new();
    let mut i = skip_ws_and_comments(chars, array_items_start);
    while i < chars.len() {
        if chars[i] == ']' {
            break;
        }
        assert!(chars[i] == '[', "i18n 门禁:pattern 项应以 [ 开头 @{i}");
        i = skip_ws_and_comments(chars, i + 1);
        assert!(
            chars.get(i) == Some(&'/'),
            "i18n 门禁:pattern 项应以 /regex/ 开头 @{i}"
        );
        // 读正则体(转义与字符类感知)
        let mut j = i + 1;
        let mut in_class = false;
        while j < chars.len() {
            match chars[j] {
                '\\' => j += 1,
                '[' if !in_class => in_class = true,
                ']' if in_class => in_class = false,
                '/' if !in_class => break,
                _ => {}
            }
            j += 1;
        }
        bodies.push(chars[i + 1..j].iter().collect());
        j += 1;
        while j < chars.len() && chars[j].is_ascii_alphabetic() {
            j += 1; // flags
        }
        // 跳过本项剩余部分(替换函数),停在项的 ']' 之后
        j = skip_js_balanced(chars, j);
        i = skip_ws_and_comments(chars, j);
        if chars.get(i) == Some(&',') {
            i = skip_ws_and_comments(chars, i + 1);
        }
    }
    bodies
}

/// 从 `from` 起扫描到「项首 [ 的配对 ]」之后。
/// 模式栈:Code(层内 {([ 深度) / 单引号 / 双引号 / 模板串;模板的 ${ 入代码层。
fn skip_js_balanced(chars: &[char], from: usize) -> usize {
    #[derive(PartialEq, Clone, Copy)]
    enum Mode {
        Code(usize),
        Sq,
        Dq,
        Tpl,
    }
    let mut stack: Vec<Mode> = vec![Mode::Code(1)]; // 项的 [ 记一层
    let mut i = from;
    while i < chars.len() {
        let c = chars[i];
        if stack.is_empty() {
            panic!("i18n 门禁:pattern 项内括号不配对 @{i}");
        }
        let top = stack.pop().unwrap();
        match top {
            Mode::Code(mut d) => {
                match c {
                    '"' => {
                        stack.push(Mode::Code(d));
                        stack.push(Mode::Dq);
                    }
                    '\'' => {
                        stack.push(Mode::Code(d));
                        stack.push(Mode::Sq);
                    }
                    '`' => {
                        stack.push(Mode::Code(d));
                        stack.push(Mode::Tpl);
                    }
                    '{' | '(' | '[' => {
                        d += 1;
                        stack.push(Mode::Code(d));
                    }
                    '}' | ')' | ']' => {
                        d -= 1;
                        if d == 0 {
                            if c == ']' {
                                return i + 1; // 本项闭合
                            }
                            // ${} 或函数层闭合:回到下层(基层配平失败=括号不配对)
                            if stack.is_empty() {
                                panic!("i18n 门禁:pattern 项内括号不配对 @{i}");
                            }
                        } else {
                            stack.push(Mode::Code(d));
                        }
                    }
                    '/' if chars.get(i + 1) == Some(&'/') => {
                        stack.push(Mode::Code(d));
                        while i < chars.len() && chars[i] != '\n' {
                            i += 1;
                        }
                    }
                    _ => stack.push(Mode::Code(d)),
                }
            }
            Mode::Sq => match c {
                '\\' => {
                    i += 1;
                    stack.push(Mode::Sq);
                }
                '\'' => {}
                _ => stack.push(Mode::Sq),
            },
            Mode::Dq => match c {
                '\\' => {
                    i += 1;
                    stack.push(Mode::Dq);
                }
                '"' => {}
                _ => stack.push(Mode::Dq),
            },
            Mode::Tpl => {
                match c {
                    '\\' => {
                        i += 1;
                        stack.push(Mode::Tpl);
                    }
                    '`' => {}
                    '$' if chars.get(i + 1) == Some(&'{') => {
                        stack.push(Mode::Tpl);
                        stack.push(Mode::Code(1)); // ${ ... } 内部代码层
                        i += 1; // '${' 的 '{' 由本层吃掉,不许再当开括号计一次
                    }
                    _ => stack.push(Mode::Tpl),
                }
            }
        }
        i += 1;
    }
    panic!("i18n 门禁:pattern 项未闭合 @{from}")
}

fn find_sub(chars: &[char], needle: &str, from: usize) -> usize {
    let n: Vec<char> = needle.chars().collect();
    chars[from..]
        .windows(n.len())
        .position(|w| w == n.as_slice())
        .map(|p| p + from)
        .unwrap_or_else(|| panic!("i18n 门禁:找不到 {needle:?}"))
}

// ---------------------------------------------------------------- 覆盖判定

const HOLE: char = '\u{E000}';
/// 探针:代入模板洞后的具体值。空串覆盖「洞在开头」的前缀类句式,
/// "2026-01-01" 覆盖日期类 pattern,"1.2K"/"1"/"12:00" 覆盖数字时刻类。
const PROBES: [&str; 5] = ["", "1", "1.2K", "2026-01-01", "12:00"];

struct Coverage {
    dict: Vec<String>,
    prefix: Vec<String>,
    patterns: Vec<Regex>,
}

impl Coverage {
    fn extract(src: &str) -> Coverage {
        let chars: Vec<char> = src.chars().collect();
        let i18n_body = extract_object_body(&chars, "I18N");
        let prefix_body = extract_object_body(&chars, "I18N_PREFIX");
        let dict = parse_object_pairs(&i18n_body)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let prefix = parse_object_pairs(&prefix_body)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let pats_needle = "const I18N_PATTERNS = [";
        let pats_at = find_sub(&chars, pats_needle, 0) + pats_needle.chars().count();
        let patterns = extract_pattern_bodies(&chars, pats_at)
            .into_iter()
            .map(|b| Regex::new(&b))
            .collect();
        Coverage {
            dict,
            prefix,
            patterns,
        }
    }

    /// 一个运行时文本节点 / 属性值是否会被 translateOne 翻译(语义对齐:
    /// 先 trim,再整句 → 正则 → 前缀;raw 形态一并试,便于拼接片段过闸)。
    fn covers_node(&self, text: &str) -> bool {
        let t = text.trim();
        if t.is_empty() {
            return true;
        }
        if self.dict.iter().any(|k| k == t || k == text) {
            return true;
        }
        for re in &self.patterns {
            if re.full_match(t) || re.full_match(text) {
                return true;
            }
        }
        if self
            .prefix
            .iter()
            .any(|k| t.starts_with(k) || text.starts_with(k))
        {
            return true;
        }
        false
    }

    /// 模板骨架(${...} 已挖成 HOLE)是否可被覆盖:探针代入后按节点判。
    fn covers_skeleton(&self, skeleton: &str) -> bool {
        if self.covers_node(skeleton) {
            return true;
        }
        let holes = skeleton.matches(HOLE).count();
        if holes == 0 {
            return false;
        }
        let probes = if holes <= 4 {
            cartesian_fill(skeleton, holes)
        } else {
            PROBES.iter().map(|p| skeleton.replace(HOLE, p)).collect()
        };
        probes.iter().any(|cand| self.covers_node(cand))
    }
}

/// 每个洞独立代入 5 种探针(5^4=625 上限,串都很短,足够便宜)。
fn cartesian_fill(skeleton: &str, holes: usize) -> Vec<String> {
    let mut acc: Vec<String> = vec![skeleton.to_string()];
    for _ in 0..holes {
        let mut next = Vec::new();
        for base in &acc {
            for p in PROBES {
                let idx = base.find(HOLE).unwrap();
                let mut s = String::with_capacity(base.len() + p.len());
                s.push_str(&base[..idx]);
                s.push_str(p);
                s.push_str(&base[idx + HOLE.len_utf8()..]);
                next.push(s);
            }
        }
        acc = next;
    }
    acc
}

// ---------------------------------------------------------------- 通用工具

fn is_cjk(c: char) -> bool {
    ('\u{4e00}'..='\u{9fff}').contains(&c)
}

fn has_cjk(s: &str) -> bool {
    s.chars().any(is_cjk)
}

fn decode_entities(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '&' {
            let rest: String = chars[i..].iter().take(12).collect();
            if let Some(semi) = rest.find(';') {
                let name = &rest[1..semi];
                let mapped = match name {
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "amp" => Some('&'),
                    "quot" => Some('"'),
                    "#39" | "apos" => Some('\''),
                    "nbsp" => Some('\u{a0}'),
                    _ => {
                        if let Some(num) = name.strip_prefix('#') {
                            let hex = num.strip_prefix('x').or_else(|| num.strip_prefix('X'));
                            let code = match hex {
                                Some(h) => u32::from_str_radix(h, 16).ok(),
                                None => num.parse::<u32>().ok(),
                            };
                            code.and_then(char::from_u32)
                        } else {
                            None
                        }
                    }
                };
                if let Some(ch) = mapped {
                    out.push(ch);
                    i += semi + 1;
                    continue;
                }
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

// ---------------------------------------------------------------- A. 静态 HTML

const VOID_TAGS: [&str; 13] = [
    "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source",
    "track",
];

struct Finding {
    line: usize,
    text: String,
}

/// 遍历静态文本节点;跳过 script/style 内容与 data-no-i18n 子树。
/// 同时抽出各 <script> 区域(char 偏移 + 体)供 B 段复用。
fn walk_static_and_scripts(src: &str) -> (Vec<Finding>, Vec<(usize, String)>) {
    let chars: Vec<char> = src.chars().collect();
    let mut line_starts: Vec<usize> = vec![0];
    for (i, c) in chars.iter().enumerate() {
        if *c == '\n' {
            line_starts.push(i + 1);
        }
    }
    let line_of = |ci: usize| line_starts.partition_point(|&p| p <= ci);

    let mut findings = Vec::new();
    let mut scripts = Vec::new();
    let mut stack: Vec<(String, bool)> = Vec::new();
    let mut no_i18n = 0usize;
    let mut i = 0usize;
    let mut text_start = 0usize;

    while i < chars.len() {
        if chars[i] != '<' {
            i += 1;
            continue;
        }
        // 先结算这段文本
        if no_i18n == 0 && text_start < i {
            let raw: String = chars[text_start..i].iter().collect();
            if has_cjk(&raw) {
                let decoded = decode_entities(&raw);
                if has_cjk(&decoded) {
                    findings.push(Finding {
                        line: line_of(text_start),
                        text: decoded.trim().to_string(),
                    });
                }
            }
        }
        if chars[i..].starts_with(&['<', '!', '-', '-']) {
            let mut j = i + 4;
            while j + 2 < chars.len()
                && !(chars[j] == '-' && chars[j + 1] == '-' && chars[j + 2] == '>')
            {
                j += 1;
            }
            i = (j + 3).min(chars.len());
            text_start = i;
            continue;
        }
        if chars[i..].starts_with(&['<', '!']) {
            // <!DOCTYPE …> 等声明:直接跳到 '>'
            while i < chars.len() && chars[i] != '>' {
                i += 1;
            }
            i += 1;
            text_start = i;
            continue;
        }
        if chars[i..].starts_with(&['<', '/']) {
            let mut j = i + 2;
            let mut name = String::new();
            while j < chars.len() && (chars[j].is_ascii_alphanumeric() || chars[j] == '-') {
                name.push(chars[j]);
                j += 1;
            }
            while j < chars.len() && chars[j] != '>' {
                j += 1;
            }
            while let Some((top, flagged)) = stack.pop() {
                if flagged {
                    no_i18n -= 1;
                }
                if top == name {
                    break;
                }
            }
            i = j + 1;
            text_start = i;
            continue;
        }
        // 开标签
        let mut j = i + 1;
        let mut name = String::new();
        while j < chars.len() && (chars[j].is_ascii_alphanumeric() || chars[j] == '-') {
            name.push(chars[j]);
            j += 1;
        }
        let attr_start = j;
        let mut in_q: Option<char> = None;
        while j < chars.len() {
            let c = chars[j];
            match in_q {
                Some(q) => {
                    if c == q {
                        in_q = None;
                    }
                }
                None => {
                    if c == '"' || c == '\'' {
                        in_q = Some(c);
                    } else if c == '>' {
                        break;
                    }
                }
            }
            j += 1;
        }
        let attr_text: String = chars[attr_start..j.min(chars.len())].iter().collect();
        let self_closing = j > 0 && chars[j - 1] == '/';
        let flagged = attr_text.contains("data-no-i18n");
        i = j + 1;
        if name == "script" || name == "style" {
            let close: Vec<char> = format!("</{name}").chars().collect();
            let mut k = i;
            while k < chars.len() && !chars[k..].starts_with(close.as_slice()) {
                k += 1;
            }
            if name == "script" {
                scripts.push((i, chars[i..k].iter().collect()));
            }
            i = k;
            text_start = i;
            continue;
        }
        if !self_closing && !VOID_TAGS.contains(&name.as_str()) {
            stack.push((name, flagged));
            if flagged {
                no_i18n += 1;
            }
        }
        text_start = i;
    }
    if no_i18n == 0 && text_start < chars.len() {
        let raw: String = chars[text_start..].iter().collect();
        if has_cjk(&raw) {
            let decoded = decode_entities(&raw);
            if has_cjk(&decoded) {
                findings.push(Finding {
                    line: line_of(text_start),
                    text: decoded.trim().to_string(),
                });
            }
        }
    }
    (findings, scripts)
}

// ---------------------------------------------------------------- B. JS 字面量

/// 字符级扫描:剥注释、跳正则字面量,收含 CJK 的 '...' "..." `...`;
/// 模板 ${...} 挖成 HOLE。词典区 [dict_from, dict_to) 内的字面量跳过。
/// 偏移一律用 char 下标(与 walk_static_and_scripts 的区域偏移同口径)。
fn scan_js_literals(scripts: &[(usize, String)], dict_from: usize, dict_to: usize) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (base, body) in scripts {
        // 体内 char i 的绝对 char 偏移(与 base 同口径,按 char 计数)
        let mut char_off: Vec<usize> = Vec::with_capacity(body.chars().count() + 1);
        for (i, _) in body.chars().enumerate() {
            char_off.push(base + i);
        }
        let chars: Vec<char> = body.chars().collect();
        let mut i = 0usize;
        let mut quote: Option<char> = None;
        let mut buf: Vec<char> = Vec::new();
        let mut lit_start: usize = 0;
        while i < chars.len() {
            let c = chars[i];
            match quote {
                None => {
                    if c == '/' && chars.get(i + 1) == Some(&'/') {
                        while i < chars.len() && chars[i] != '\n' {
                            i += 1;
                        }
                        continue;
                    }
                    if c == '/' && chars.get(i + 1) == Some(&'*') {
                        i += 2;
                        while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                            i += 1;
                        }
                        i += 2;
                        continue;
                    }
                    if c == '/' {
                        // 正则字面量粗判:前一非空白字符
                        let mut j = i;
                        let mut prev = ' ';
                        while j > 0 {
                            j -= 1;
                            if !chars[j].is_whitespace() {
                                prev = chars[j];
                                break;
                            }
                        }
                        if "=([:!&|?{};,'".contains(prev) {
                            i += 1;
                            let mut in_cls = false;
                            while i < chars.len() {
                                if chars[i] == '\\' {
                                    i += 2;
                                    continue;
                                }
                                if chars[i] == '[' {
                                    in_cls = true;
                                } else if chars[i] == ']' {
                                    in_cls = false;
                                } else if chars[i] == '/' && !in_cls {
                                    break;
                                }
                                i += 1;
                            }
                            i += 1;
                            continue;
                        }
                    }
                    if c == '"' || c == '\'' || c == '`' {
                        quote = Some(c);
                        buf.clear();
                        lit_start = char_off[i];
                    }
                }
                Some(q) => {
                    if c == '\\' {
                        let e = chars.get(i + 1).copied().unwrap_or('\\');
                        match e {
                            'n' => buf.push('\n'),
                            't' => buf.push('\t'),
                            'r' => buf.push('\r'),
                            'u' => {
                                let hex: String =
                                    chars[i + 2..(i + 6).min(chars.len())].iter().collect();
                                if let Some(ch) =
                                    u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32)
                                {
                                    buf.push(ch);
                                    i += 6;
                                    continue;
                                }
                                buf.push('u');
                            }
                            other => buf.push(other),
                        }
                        i += 2;
                        continue;
                    }
                    if q == '`' && c == '$' && chars.get(i + 1) == Some(&'{') {
                        // 挖洞:整段 ${...}(字符串感知)换成 HOLE
                        let mut depth = 1usize;
                        let mut j = i + 2;
                        while j < chars.len() && depth > 0 {
                            match chars[j] {
                                '{' => depth += 1,
                                '}' => depth -= 1,
                                '"' | '\'' | '`' => {
                                    let q2 = chars[j];
                                    j += 1;
                                    while j < chars.len() && chars[j] != q2 {
                                        if chars[j] == '\\' {
                                            j += 1;
                                        }
                                        j += 1;
                                    }
                                }
                                _ => {}
                            }
                            j += 1;
                        }
                        buf.push(HOLE);
                        i = j;
                        continue;
                    }
                    if c == q {
                        let skeleton: String = buf.iter().collect();
                        let abs = lit_start;
                        quote = None;
                        if has_cjk(&skeleton)
                            && !(dict_from..dict_to).contains(&abs)
                            && seen.insert(skeleton.clone())
                        {
                            findings.push(Finding {
                                line: doc_line_of(abs),
                                text: skeleton,
                            });
                        }
                        buf.clear();
                    } else {
                        buf.push(c);
                    }
                }
            }
            i += 1;
        }
    }
    findings
}

/// 绝对 char 偏移 → 全文档行号。
fn doc_line_of(abs: usize) -> usize {
    static DOC: std::sync::OnceLock<Vec<usize>> = std::sync::OnceLock::new();
    let starts = DOC.get_or_init(|| {
        let src = include_str!("dashboard.html");
        let chars: Vec<char> = src.chars().collect();
        let mut v = vec![0];
        for (i, c) in chars.iter().enumerate() {
            if *c == '\n' {
                v.push(i + 1);
            }
        }
        v
    });
    starts.partition_point(|&p| p <= abs)
}

// ---------------------------------------------------------------- 块切与候选

struct Block {
    /// 整块原文(标签块含尖括号;文本块为近似文本节点)
    whole: String,
    /// 块内双引号里的候选(HTML 属性值)
    quoted: Vec<String>,
}

/// 把骨架按标签边界切块;每块附引号内候选。
fn blocks_of(skeleton: &str) -> Vec<Block> {
    let chars: Vec<char> = skeleton.chars().collect();
    let mut blocks: Vec<Block> = Vec::new();
    let mut cur = String::new();
    let mut cur_quoted: Vec<String> = Vec::new();
    let push = |cur: &mut String, q: &mut Vec<String>, blocks: &mut Vec<Block>| {
        if !cur.trim().is_empty() || !q.is_empty() {
            blocks.push(Block {
                whole: std::mem::take(cur),
                quoted: std::mem::take(q),
            });
        } else {
            cur.clear();
            q.clear();
        }
    };
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '<' {
            if let Some(gt) = chars[i..].iter().position(|&c| c == '>') {
                push(&mut cur, &mut cur_quoted, &mut blocks);
                let tag: String = chars[i..i + gt + 1].iter().collect();
                blocks.push(Block {
                    whole: tag.clone(),
                    quoted: extract_quoted_runs(&tag),
                });
                i += gt + 1;
                continue;
            }
        }
        cur.push(chars[i]);
        if chars[i] == '"' {
            let mut k = i + 1;
            let mut run = String::new();
            while k < chars.len() && chars[k] != '"' {
                run.push(chars[k]);
                k += 1;
            }
            cur.push_str(&run);
            if k < chars.len() {
                cur.push('"');
            }
            cur_quoted.push(run);
            i = k + 1;
            continue;
        }
        i += 1;
    }
    push(&mut cur, &mut cur_quoted, &mut blocks);
    blocks
}

fn extract_quoted_runs(s: &str) -> Vec<String> {
    let chars: Vec<char> = s.chars().collect();
    let mut runs = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '"' {
            let mut k = i + 1;
            let mut run = String::new();
            while k < chars.len() && chars[k] != '"' {
                run.push(chars[k]);
                k += 1;
            }
            runs.push(run);
            i = k + 1;
            continue;
        }
        i += 1;
    }
    runs
}

fn strip_quoted(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '"' {
            let mut k = i + 1;
            while k < chars.len() && chars[k] != '"' {
                k += 1;
            }
            i = k + 1;
            continue;
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// 一段骨架是否通过:整块可译 → 过;否则引号外不得有中文,且所有含中文的
/// 引号候选(属性值,运行时被 applyI18n 的 [title]/[placeholder] 通道单独翻)逐个可译。
fn skeleton_passes(cov: &Coverage, skeleton: &str) -> bool {
    for block in blocks_of(skeleton) {
        if cov.covers_skeleton(&block.whole) {
            continue;
        }
        if has_cjk(&strip_quoted(&block.whole)) {
            return false;
        }
        if block
            .quoted
            .iter()
            .any(|r| has_cjk(r) && !cov.covers_skeleton(r))
        {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------- allowlist

/// 显式豁免(骨架文本,HOLE = 模板洞)。逐条理由,只进不出:
/// 新增时必须写清「为什么词典/正则管不到它,以及运行时由谁兜住」。
const ALLOWLIST: &[&str] = &[
    // 语言切换按钮:值就是目标语言「中」,不该被翻译
    "中",
    // —— 拼接片段:与相邻字面量组成一个文本节点,整节点由既有 pattern 覆盖 ——
    // 检索质量行(整行 pattern 的 replace 分支负责这两段)
    " · 已合并 \u{E000} 条近似回复",
    " · 软AND兜底 \u{E000}",
    " · 最近坏查询: ",
    // 网关排行副行:与 fmtTokens 前半拼接成「1.2M · 3 估算」整节点
    " · \u{E000} 估算",
    "「\u{E000}」(\u{E000}条)",
    // syncSummary:net 槽位是 净增/净减/总量持平 三选一(三条整行 pattern),
    // errTail 是行尾可选组 —— 探针代不进中文枚举值
    "净\u{E000} \u{E000}",
    " · \u{E000} 个来源失败",
    "全量重建完成 · 重新采集 \u{E000} 条 · \u{E000}（\u{E000} → \u{E000}）· \u{E000}s\u{E000}",
    // 网关摘要:live 片段并入「监听 …」整行 pattern
    " · 本次运行已经手 \u{E000} 笔请求(\u{E000} 错)",
    // Fleet 推/拉:label ∈ {推送,拉取},整节点由 Fleet 推送中…/拉取中… pattern 覆盖
    "Fleet \u{E000}中…",
    "Fleet \u{E000}失败：\u{E000}",
    // 页脚:版本洞含 "TokenBuddy vN" 前缀,整节点由 footer 两条 pattern 覆盖
    "\u{E000} · 数据来自 \u{E000} 本地日志",
    "\u{E000} · 本机未检测到已支持工具的本地日志",
    // —— 不走 DOM 的串(applyI18n 机制上够不着)——
    // resultsToText:复制到剪贴板的纯文本
    "[\u{E000}] \u{E000} · \u{E000}\u{E000} · \u{E000} · \u{E000} · 相关度 \u{E000}",
    // exportResultsMd:下载的 Markdown 文件头与文件名(issue 附注:另议)
    "# TokenBuddy 上下文搜索结果",
    "- **查询**：\u{E000}",
    "- **时间**：\u{E000}",
    "- **命中**：\u{E000} 条 · 耗时 \u{E000}ms",
    "## \u{E000}. [\u{E000}] \u{E000}",
    // 角色后第二洞 = #60 产物命中的嵌套模板(整块算一洞)
    "- 项目：\u{E000}\u{E000} · 角色：\u{E000}\u{E000} · 时间：\u{E000} · 相关度：\u{E000}",
    "文件：\u{E000}",
    "TokenBuddy搜索_\u{E000}_\u{E000}.md",
    // 产物预览说明段:位于 artBody(data-no-i18n 用户内容保护区)内,
    // 运行时不可达;对应死词条已删,整段翻译等产物视图重构时一并处理
    "<p>这是一个 <strong>HTML</strong> 产物。",
    "本页与 TokenBuddy 的 API 同源，在页内渲染本地 HTML 等于把它放到所有 API 旁边，",
    "所以这里不放内嵌预览——点右上角「在浏览器中打开」用系统浏览器看。</p>",
];

// ---------------------------------------------------------------- 门禁测试

fn run_gate() -> Result<(), String> {
    let src = include_str!("dashboard.html");
    let cov = Coverage::extract(src);
    if cov.dict.is_empty() || cov.patterns.is_empty() {
        return Err("词典/pattern 抽取为空,抽取器坏了".to_string());
    }

    // 词典区 char 区间:B 段扫描跳过 I18N/I18N_PREFIX/I18N_PATTERNS 本体
    let chars: Vec<char> = src.chars().collect();
    let dict_from = find_sub(&chars, "const I18N = {", 0);
    let pats = find_sub(&chars, "const I18N_PATTERNS", 0);
    let dict_to = find_sub(&chars, "\n];", pats);

    let mut errors: Vec<String> = Vec::new();

    let (static_findings, scripts) = walk_static_and_scripts(src);
    for f in &static_findings {
        if !cov.covers_node(&f.text) {
            errors.push(format!("  A L{}: {:?}", f.line, f.text));
        }
    }

    for f in scan_js_literals(&scripts, dict_from, dict_to) {
        if ALLOWLIST.contains(&f.text.as_str()) {
            continue;
        }
        if !skeleton_passes(&cov, &f.text) {
            errors.push(format!("  B L{}: {:?}", f.line, f.text));
        }
    }

    if errors.is_empty() {
        return Ok(());
    }
    Err(format!(
        "i18n 门禁发现 {} 处 EN 模式漏翻(A=静态文本节点, B=JS 动态串):\n{}",
        errors.len(),
        errors.join("\n")
    ))
}

#[test]
fn dashboard_i18n_covers_all_user_facing_chinese() {
    if let Err(msg) = run_gate() {
        panic!("{msg}");
    }
}

// ------------------------------------------------ 迷你正则引擎自身的单测

#[test]
fn i18n_gate_regex_engine_basics() {
    let re = Regex::new("^已加载 ([\\d,]+) 条记录$");
    assert!(re.full_match("已加载 1,234 条记录"));
    assert!(!re.full_match("已加载 x 条记录"));

    let re = Regex::new("^(\\d{1,2}):00–(\\d{1,2}):59\\n(.+) tokens · ([\\d,]+) 请求$");
    assert!(re.full_match("9:00–9:59\n1.2K tokens · 12 请求"));
    assert!(!re.full_match("9:00–9:59 1.2K tokens · 12 请求"));

    let re = Regex::new("^缓存读 (.+) · 写 (.+?)(?: · 子代理 (.+))?$");
    assert!(re.full_match("缓存读 1 · 写 2"));
    assert!(re.full_match("缓存读 1 · 写 2 · 子代理 3"));

    let re = Regex::new("^按当前燃速 (.+)\\/h,约 (.+)到达 P90 节奏$");
    assert!(re.full_match("按当前燃速 45K/h,约 45 分钟到达 P90 节奏"));

    let re = Regex::new("^([\\w./:-]+): (.+) tokens · ([\\d,]+) 请求$");
    assert!(re.full_match("claude: 1.2M tokens · 152 请求"));

    let re = Regex::new("^TokenBuddy( v.+?)? · 数据来自 (.+) 本地日志$");
    assert!(re.full_match("TokenBuddy v0.11.1 · 数据来自 Claude / Zcode 本地日志"));
    assert!(re.full_match("TokenBuddy · 数据来自 Claude 本地日志"));

    let re = Regex::new("^没有命中「(.+)」——尝试更短的关键词，或放宽来源\\/时间过滤$");
    assert!(re.full_match("没有命中「测试」——尝试更短的关键词，或放宽来源/时间过滤"));
}

#[test]
fn i18n_gate_regex_rejects_unanchored() {
    let result = std::panic::catch_unwind(|| Regex::new("没有锚点的(.+)模式"));
    assert!(result.is_err(), "未锚定 pattern 必须被拒绝");
}
