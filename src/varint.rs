fn encode_to_vec(mut n: u32) -> Vec<u8> {
    if n <= 250 {
        return vec![n as u8];
    }

    let mut v: Vec<u8> = Vec::with_capacity(5);
    while n > 0 {
        v.push(n as u8);
        n >>= 8;
    }
    v.push(250 + v.len() as u8);
    v.reverse();
    v
}

fn decode_from_vec(v: Vec<u8>) -> Option<u32> {
    let (&first, rest) = v.split_first()?;
    let first = first;
    if first <= 250 {
        return Some(first as u32);
    }
    let mut n: u32 = 0;
    for i in 0 .. (first - 250) {
        n = (n << 8) + *rest.get(i as usize)? as u32;
    }
    Some(n)
}

#[cfg(test)]
mod test {
    use crate::varint::{decode_from_vec, encode_to_vec};

    #[test]
    fn encode_golden() {
        assert_eq!(encode_to_vec(0), vec![0]);
        assert_eq!(encode_to_vec(250), vec![250]);
        assert_eq!(encode_to_vec(251), vec![251, 251]);
        assert_eq!(encode_to_vec(255), vec![251, 255]);
        assert_eq!(encode_to_vec(256), vec![252, 1, 0]);
    }

    #[test]
    fn decode_golden() {
        assert_eq!(decode_from_vec(vec![0]), Some(0));
        assert_eq!(decode_from_vec(vec![250]), Some(250));
        assert_eq!(decode_from_vec(vec![251, 251]), Some(251));
        assert_eq!(decode_from_vec(vec![252, 1, 0]), Some(256));

        // Some broken decodings
        assert_eq!(decode_from_vec(vec![251]), None);
        assert_eq!(decode_from_vec(vec![253, 1, 0]), None);

        // Some weird but technically valid encodings
        assert_eq!(decode_from_vec(vec![251, 0]), Some(0));
        assert_eq!(decode_from_vec(vec![252, 0, 1]), Some(1));
    }
}