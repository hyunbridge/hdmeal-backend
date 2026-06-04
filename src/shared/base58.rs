//! Base58 encoder/decoder — `bs58` crate 래퍼.
//!
//! 호환을 위해 보존. 내부 구현은 `bs58` crate 에 위임합니다.
//! 내부 구현은 `bs58` crate 에 위임합니다.

use std::sync::LazyLock;

const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

static CODEC: LazyLock<bs58::Alphabet> =
    LazyLock::new(|| bs58::Alphabet::new(ALPHABET).expect("hardcoded alphabet is valid"));

pub fn encode(input: &[u8]) -> String {
    bs58::encode(input).with_alphabet(&CODEC).into_string()
}

pub fn decode(input: &str) -> Option<Vec<u8>> {
    bs58::decode(input).with_alphabet(&CODEC).into_vec().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_known_values() {
        for raw in [0u64, 1, 57, 58, 59, 12345, 999_999] {
            let bytes = raw.to_be_bytes();
            let encoded = encode(&bytes);
            let decoded = decode(&encoded).expect("decode");
            let trimmed: Vec<u8> = decoded.iter().skip_while(|&&b| b == 0).copied().collect();
            let expected: Vec<u8> = bytes.iter().skip_while(|&&b| b == 0).copied().collect();
            assert_eq!(trimmed, expected, "raw={raw}");
        }
    }

    #[test]
    fn empty() {
        assert_eq!(encode(&[]), "");
        assert_eq!(decode("").unwrap(), Vec::<u8>::new());
    }
}
