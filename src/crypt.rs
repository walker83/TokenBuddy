//! At-rest encryption for the Fleet object (`encrypt = true` in fleet.toml).
//!
//! The one place TokenBuddy data leaves the machine is the Fleet push: the
//! token ledger lands in a shared LAN bucket where anyone with bucket access
//! could read which projects ran which models at what cadence. With
//! `encrypt = true` the object is XChaCha20-Poly1305 ciphertext instead:
//! `TBENCRV1 || 24-byte random nonce || ciphertext+tag`, the content key
//! HKDF-derived from the existing S3 `secret_key` (no second secret to
//! manage), with a domain-separation AAD so the same key can't be reused
//! across protocols.
//!
//! Wire format stays unambiguous: objects without the magic header are plain
//! parquet, so old pushes and non-encrypting peers keep working.

use anyhow::{anyhow, Result};
use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::{aead::AeadCore, XChaCha20Poly1305, XNonce};

/// Magic header + format version: one byte each doesn't matter, one string
/// that spells itself in a hex dump does.
pub const MAGIC: &[u8; 8] = b"TBENCRV1";
/// Binds the ciphertext to this protocol so a key reused elsewhere can't be
/// replayed here, and vice versa.
const AAD: &[u8] = b"tokenbuddy-fleet-data-v1";

/// Derive the 32-byte content key from the Fleet secret via HKDF-SHA256.
/// Deterministic (pull side needs no key material beyond fleet.toml) but
/// bound to this exact use.
pub fn derive_key(secret_key: &str) -> Result<[u8; 32]> {
    anyhow::ensure!(!secret_key.is_empty(), "secret_key 为空，无法派生加密密钥");
    let salt = b"tokenbuddy-fleet-at-rest";
    let hk = hkdf::Hkdf::<sha2::Sha256>::new(Some(salt), secret_key.as_bytes());
    let mut key = [0u8; 32];
    hk.expand(AAD, &mut key)
        .map_err(|_| anyhow!("HKDF expand failed"))?;
    Ok(key)
}

pub fn is_encrypted(blob: &[u8]) -> bool {
    blob.len() > MAGIC.len() && &blob[..MAGIC.len()] == MAGIC
}

/// Encrypt to `MAGIC || nonce || ciphertext(+16-byte Poly1305 tag)`.
pub fn encrypt(plaintext: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    let cipher = XChaCha20Poly1305::new_from_slice(key).map_err(|_| anyhow!("key length error"))?;
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let ct = cipher
        .encrypt(
            &nonce,
            chacha20poly1305::aead::Payload {
                msg: plaintext,
                aad: AAD,
            },
        )
        .map_err(|_| anyhow!("encrypt failed"))?;
    let mut out = Vec::with_capacity(MAGIC.len() + nonce.len() + ct.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(nonce.as_slice());
    out.extend_from_slice(&ct);
    Ok(out)
}

/// Decrypt a blob produced by [`encrypt`]; any tampering or wrong key fails
/// the Poly1305 tag check rather than yielding garbage.
pub fn decrypt(blob: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    if !is_encrypted(blob) {
        return Err(anyhow!("对象不是 TokenBuddy 加密格式（缺少 TBENCRV1 头）"));
    }
    const NONCE_LEN: usize = 24;
    if blob.len() < MAGIC.len() + NONCE_LEN + 16 {
        return Err(anyhow!("加密对象被截断"));
    }
    let cipher = XChaCha20Poly1305::new_from_slice(key).map_err(|_| anyhow!("key length error"))?;
    let nonce = XNonce::from_slice(&blob[MAGIC.len()..MAGIC.len() + NONCE_LEN]);
    cipher
        .decrypt(
            nonce,
            chacha20poly1305::aead::Payload {
                msg: &blob[MAGIC.len() + NONCE_LEN..],
                aad: AAD,
            },
        )
        .map_err(|_| anyhow!("解密失败：secret_key 不匹配或对象被篡改"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_tamper_detection() {
        let key = derive_key("s3-secret").unwrap();
        let plain = vec![7u8; 100_000];
        let blob = encrypt(&plain, &key).unwrap();
        assert!(is_encrypted(&blob));
        assert_eq!(decrypt(&blob, &key).unwrap(), plain);

        // Flip one bit in the ciphertext → tag check fails, no garbage out.
        let mut tampered = blob.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert!(decrypt(&tampered, &key).is_err());

        // Wrong key fails; the right key derived later keeps working.
        let other = derive_key("another-secret").unwrap();
        assert!(decrypt(&blob, &other).is_err());
        assert_eq!(
            decrypt(&encrypt(&plain, &key).unwrap(), &key).unwrap(),
            plain
        );

        // Plain parquet (no magic) is reported as such, not half-decoded.
        assert!(!is_encrypted(&plain));
        assert!(decrypt(&plain, &key).is_err());

        // Random nonce: same plaintext never produces the same object.
        let blob2 = encrypt(&plain, &key).unwrap();
        assert_ne!(blob, blob2);
        // But both decrypt to the same bytes.
        assert_eq!(decrypt(&blob2, &key).unwrap(), plain);
    }

    #[test]
    fn key_derivation_is_deterministic_and_domain_bound() {
        let a = derive_key("alpha").unwrap();
        let a2 = derive_key("alpha").unwrap();
        let b = derive_key("beta").unwrap();
        assert_eq!(a, a2, "pull side must re-derive the same key");
        assert_ne!(a, b, "different secrets diverge");
    }

    #[test]
    fn empty_secret_is_rejected() {
        assert!(derive_key("").is_err());
    }

    #[test]
    fn encrypted_push_roundtrips_through_fleet_format() {
        // End-to-end at the fleet-object level: what push() would write and
        // what pull_all_into() would read, without needing an S3 endpoint.
        let cfg_key = derive_key("shared-secret").unwrap();
        let plain = b"PARQUET-CONTENT-HERE";
        let body = encrypt(plain, &cfg_key).unwrap();

        // Pull side: detect + decrypt with the same config, reject others.
        assert!(is_encrypted(&body));
        assert_eq!(decrypt(&body, &cfg_key).unwrap(), plain.to_vec());
        let wrong = derive_key("typo").unwrap();
        let err = decrypt(&body, &wrong).unwrap_err().to_string();
        assert!(err.contains("secret_key"), "error names the key: {err}");
    }
}
