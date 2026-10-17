pub fn encode_message(payload: &str) -> Vec<u8> {
    // Processing incoming payload
    let payload = payload.as_bytes();
    let len = payload.len() as u32;
    let len_bytes = len.to_be_bytes();

    let mut result = len_bytes.to_vec();
    result.extend_from_slice(payload);

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_encoding_result() {
        let msg = "This journey starts from nowhere";

        let result = encode_message(msg);

        println!("Result: {result:?}");
    }

    #[test]
    fn encode_length_prefixed_message() {
        let msg = "hello";

        let result = encode_message(msg);

        assert_eq!(result, vec![0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o']);
    }
}
