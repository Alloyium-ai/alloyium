use base64::{engine::general_purpose::STANDARD, Engine};
use ed25519_dalek::{Signer, SigningKey};
use std::convert::TryInto;
use std::error::Error;
use std::fs;
use std::io::{self, ErrorKind};
use std::sync::{Mutex, OnceLock};

const SIGNING_KEY_ENV: &str = "A2A_SIGNING_KEY";

struct CachedSigningKey {
    path: String,
    key: SigningKey,
}

static SIGNING_KEY_CACHE: OnceLock<Mutex<Option<CachedSigningKey>>> = OnceLock::new();

pub fn load_seed(path: &str) -> Result<[u8; 32], Box<dyn Error>> {
    let bytes = fs::read(path)?;

    if bytes.len() == 32 {
        return bytes_to_32(bytes, "raw signing seed");
    }

    let trimmed = trim_ascii_whitespace(&bytes);
    let text = std::str::from_utf8(trimmed).map_err(|err| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("signing seed file is neither 32 raw bytes nor valid UTF-8 base64 text: {err}"),
        )
    })?;

    let decoded = STANDARD.decode(text).map_err(|err| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid base64 signing seed: {err}"),
        )
    })?;

    bytes_to_32(decoded, "decoded signing seed")
}

pub fn sign_pop(nonce_b64: &str) -> Result<String, Box<dyn Error>> {
    let nonce = decode_pop_nonce(nonce_b64)?;
    with_cached_signing_key(|key| Ok(sign_bytes(key, &nonce)))
}

pub fn sign_canon(canon: &str) -> Result<String, Box<dyn Error>> {
    with_cached_signing_key(|key| Ok(sign_bytes(key, canon.as_bytes())))
}

fn with_cached_signing_key<T>(
    f: impl FnOnce(&SigningKey) -> Result<T, Box<dyn Error>>,
) -> Result<T, Box<dyn Error>> {
    let path = std::env::var(SIGNING_KEY_ENV).map_err(|err| {
        io::Error::new(
            ErrorKind::NotFound,
            format!("{SIGNING_KEY_ENV} is not set or is not valid Unicode: {err}"),
        )
    })?;

    let cache = SIGNING_KEY_CACHE.get_or_init(|| Mutex::new(None));
    let mut guard = cache
        .lock()
        .map_err(|_| io::Error::new(ErrorKind::Other, "signing key cache mutex is poisoned"))?;

    let needs_load = guard
        .as_ref()
        .map(|cached| cached.path.as_str() != path.as_str())
        .unwrap_or(true);

    if needs_load {
        let seed = load_seed(&path)?;
        *guard = Some(CachedSigningKey {
            path: path.clone(),
            key: SigningKey::from_bytes(&seed),
        });
    }

    let key = &guard.as_ref().expect("signing key cache populated").key;

    f(key)
}

fn decode_pop_nonce(nonce_b64: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let nonce = STANDARD.decode(nonce_b64).map_err(|err| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid base64 PoP nonce: {err}"),
        )
    })?;

    if nonce.len() != 32 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "PoP nonce must decode to exactly 32 bytes, got {}",
                nonce.len()
            ),
        )
        .into());
    }

    Ok(nonce)
}

fn sign_bytes(key: &SigningKey, msg: &[u8]) -> String {
    let sig = key.sign(msg);
    STANDARD.encode(sig.to_bytes())
}

fn bytes_to_32(bytes: Vec<u8>, label: &str) -> Result<[u8; 32], Box<dyn Error>> {
    let len = bytes.len();
    let out = bytes.as_slice().try_into().map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("{label} must be exactly 32 bytes, got {len}"),
        )
    })?;

    Ok(out)
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = bytes.len();

    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }

    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }

    &bytes[start..end]
}
