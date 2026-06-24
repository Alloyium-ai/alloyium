use a2a_shim::signer;
use base64::{engine::general_purpose::STANDARD, Engine};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde_json::Value;
use std::convert::TryInto;
use std::fs;
use std::sync::Once;

static SIGNING_KEY_ENV_ONCE: Once = Once::new();

fn vectors() -> Value {
    let text = fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/shim-conformance/vectors.json"
    ))
    .unwrap();

    serde_json::from_str(&text).unwrap()
}

fn str_field<'a>(value: &'a Value, path: &[&str]) -> &'a str {
    let mut cursor = value;

    for key in path {
        cursor = cursor
            .get(*key)
            .unwrap_or_else(|| panic!("missing vector field {}", path.join(".")));
    }

    cursor
        .as_str()
        .unwrap_or_else(|| panic!("vector field {} is not a string", path.join(".")))
}

fn hex_decode(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0, "hex string must have even length");

    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex digit {}", byte as char),
    }
}

fn seed_from_vectors(v: &Value) -> [u8; 32] {
    hex_decode(str_field(v, &["seed_hex"]))
        .as_slice()
        .try_into()
        .unwrap()
}

fn sign_b64(seed: &[u8; 32], msg: &[u8]) -> String {
    let key = SigningKey::from_bytes(seed);
    let sig = key.sign(msg);
    STANDARD.encode(sig.to_bytes())
}

fn signature_from_b64(sig_b64: &str) -> Signature {
    let sig_bytes = STANDARD.decode(sig_b64).unwrap();
    let sig_arr: [u8; 64] = sig_bytes.as_slice().try_into().unwrap();
    Signature::from_bytes(&sig_arr)
}

fn install_signing_key_env(seed: &[u8; 32]) {
    SIGNING_KEY_ENV_ONCE.call_once(|| {
        let path =
            std::env::temp_dir().join(format!("a2a-shim-sign-vectors-{}.seed", std::process::id()));

        fs::write(&path, seed).unwrap();

        let path = path.to_str().unwrap().to_owned();
        std::env::set_var("A2A_SIGNING_KEY", path);
    });
}

fn assert_canon_case_matches(case_name: &str) {
    let v = vectors();
    let seed = seed_from_vectors(&v);
    let canon_bytes = hex_decode(str_field(&v, &[case_name, "canonical_utf8_hex"]));
    let expected = str_field(&v, &[case_name, "expected_sig_b64"]);

    assert_eq!(sign_b64(&seed, &canon_bytes), expected);

    let canon = String::from_utf8(canon_bytes).unwrap();
    install_signing_key_env(&seed);
    assert_eq!(signer::sign_canon(&canon).unwrap(), expected);
}

fn assert_verifies(key: &VerifyingKey, msg: &[u8], sig_b64: &str) {
    let sig = signature_from_b64(sig_b64);
    key.verify_strict(msg, &sig).unwrap();
}

#[test]
fn pop_decode32_matches() {
    let v = vectors();
    let seed = seed_from_vectors(&v);

    let nonce = STANDARD
        .decode(str_field(&v, &["pop", "nonce_b64"]))
        .unwrap();

    assert_eq!(nonce.len(), 32);
    assert_eq!(nonce, hex_decode(str_field(&v, &["pop", "nonce_hex"])));

    assert_eq!(
        sign_b64(&seed, &nonce),
        str_field(&v, &["pop", "expected_sig_b64"])
    );
}

#[test]
fn pop_negative_signing_b64_string_differs() {
    let v = vectors();
    let seed = seed_from_vectors(&v);
    let nonce_b64 = str_field(&v, &["pop", "nonce_b64"]);

    assert_ne!(
        sign_b64(&seed, nonce_b64.as_bytes()),
        str_field(&v, &["pop", "expected_sig_b64"])
    );
}

#[test]
fn canon_4a_matches() {
    assert_canon_case_matches("canon_4a");
}

#[test]
fn canon_4b_matches() {
    assert_canon_case_matches("canon_4b");
}

#[test]
fn pubkey_verifies() {
    let v = vectors();

    let pubkey_bytes = STANDARD.decode(str_field(&v, &["pubkey_raw_b64"])).unwrap();
    let pubkey_arr: [u8; 32] = pubkey_bytes.as_slice().try_into().unwrap();
    let key = VerifyingKey::from_bytes(&pubkey_arr).unwrap();

    let pop_nonce = STANDARD
        .decode(str_field(&v, &["pop", "nonce_b64"]))
        .unwrap();
    assert_verifies(
        &key,
        &pop_nonce,
        str_field(&v, &["pop", "expected_sig_b64"]),
    );

    for case_name in ["canon_4a", "canon_4b"] {
        let canon_bytes = hex_decode(str_field(&v, &[case_name, "canonical_utf8_hex"]));
        assert_verifies(
            &key,
            &canon_bytes,
            str_field(&v, &[case_name, "expected_sig_b64"]),
        );
    }
}
