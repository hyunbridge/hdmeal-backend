//! Base58 encoder/decoder.
//!
//! 호환을 위해 보존. 내부 구현은 `bs58` crate 에 위임합니다.

/// 0~57 까지의 base58 알파벳.
pub const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// 바이트열을 base58 문자열로 인코딩합니다.
pub fn encode(input: &[u8]) -> String {
    if input.is_empty() {
        return String::new();
    }

    let mut zeros = 0usize;
    while zeros < input.len() && input[zeros] == 0 {
        zeros += 1;
    }

    let mut encoded = vec![0u8; input.len() * 2];
    let mut output_start = encoded.len();
    for &byte in input {
        let mut carry = byte as usize;
        let mut i = encoded.len() - 1;
        while carry != 0 || i >= output_start {
            carry += 256 * encoded[i] as usize;
            encoded[i] = (carry % 58) as u8;
            carry /= 58;
            if i == 0 {
                break;
            }
            i -= 1;
        }
        output_start = i;
    }

    while output_start < encoded.len() && encoded[output_start] == 0 {
        output_start += 1;
    }

    let mut result = String::with_capacity(zeros + (encoded.len() - output_start));
    for _ in 0..zeros {
        result.push('1');
    }
    for b in &encoded[output_start..] {
        result.push(ALPHABET[*b as usize] as char);
    }
    result
}

/// base58 문자열을 바이트열로 디코딩합니다.
pub fn decode(input: &str) -> Option<Vec<u8>> {
    if input.is_empty() {
        return Some(Vec::new());
    }
    let bytes = input.as_bytes();

    let mut zeros = 0usize;
    while zeros < bytes.len() && bytes[zeros] == b'1' {
        zeros += 1;
    }

    let mut decoded = vec![0u8; bytes.len()];
    let mut output_start = decoded.len();
    for &b in bytes {
        let p = ALPHABET.iter().position(|&a| a == b)?;
        let mut carry = p;
        let mut i = decoded.len() - 1;
        while carry != 0 || i >= output_start {
            carry += 58 * decoded[i] as usize;
            decoded[i] = (carry & 0xff) as u8;
            carry >>= 8;
            if i == 0 {
                break;
            }
            i -= 1;
        }
        output_start = i;
    }

    while output_start < decoded.len() && decoded[output_start] == 0 {
        output_start += 1;
    }

    let mut result = Vec::with_capacity(zeros + (decoded.len() - output_start));
    result.extend(std::iter::repeat(0u8).take(zeros));
    result.extend_from_slice(&decoded[output_start..]);
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_known_values() {
        for raw in [0u64, 1, 57, 58, 59, 12345, 999_999].iter() {
            let bytes = raw.to_be_bytes();
            let encoded = encode(&bytes);
            let decoded = decode(&encoded).expect("decode");
            let pad = if bytes[0..bytes.len() - 8].iter().any(|&b| b != 0) {
                bytes.to_vec()
            } else {
                bytes[bytes.len() - 8..].to_vec()
            };
            let trimmed: Vec<u8> = decoded.iter().skip_while(|&&b| b == 0).copied().collect();
            let expected: Vec<u8> = pad.iter().skip_while(|&&b| b == 0).copied().collect();
            assert_eq!(trimmed, expected, "raw={raw}");
        }
    }

    #[test]
    fn empty() {
        assert_eq!(encode(&[]), "");
        assert_eq!(decode("").unwrap(), Vec::<u8>::new());
    }
}
