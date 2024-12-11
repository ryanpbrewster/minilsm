use std::io::Write;

pub fn encoded_length_u32(n: u32) -> u32 {
    if n < (1 << 7) {
        return 1;
    }
    if n < (1 << 14) {
        return 2;
    }
    if n < (1 << 21) {
        return 3;
    }
    if n < (1 << 28) {
        return 4;
    }
    return 5;
}
pub fn encoded_length_u64(n: u64) -> u32 {
    todo!()
}
pub fn encode_u32(n: u32, mut w: impl Write) -> std::io::Result<u32> {
    // 7 bits: leading 0, [7] value bits encoded directly
    if n < (1 << 7) {
        w.write_all(&[n as u8])?;
        return Ok(1);
    }
    // 14 bits: leading 10, [6, 8] value bits encoded directly
    if n < (1 << 14) {
        let (first, rest) = (n & 0b0011_1111, n >> 6);
        w.write_all(&[0b1000_0000 | first as u8, (rest >> 0) as u8])?;
        return Ok(2);
    }
    // 21 bits: leading 110, [5, 8, 8] value bits encoded directly
    if n < (1 << 21) {
        let (first, rest) = (n & 0b0001_1111, n >> 5);
        w.write_all(&[
            0b1100_0000 | first as u8,
            (rest >> 0) as u8,
            (rest >> 8) as u8,
        ])?;
        return Ok(3);
    }
    // 28 bits: leading 1110, [4, 8, 8, 8] value bits encoded directly
    if n < (1 << 28) {
        let (first, rest) = (n & 0b0000_1111, n >> 4);
        w.write_all(&[
            0b1110_0000 | first as u8,
            (rest >> 0) as u8,
            (rest >> 8) as u8,
            (rest >> 16) as u8,
        ])?;
        return Ok(4);
    }
    // 32 bits: leading 11110, [3, 8, 8, 8, 8] value bits encoded directly
    let (first, rest) = (n & 0b0000_0111, n >> 5);
    w.write_all(&[
        0b1110_0000 | first as u8,
        (rest >> 0) as u8,
        (rest >> 8) as u8,
        (rest >> 16) as u8,
    ])?;
    Ok(5)
}

pub fn encode_u64(n: u64, mut w: impl Write) -> std::io::Result<u32> {
    todo!()
}

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
    for i in 0..(first - 250) {
        n = (n << 8) + *rest.get(i as usize)? as u32;
    }
    Some(n)
}

fn prefix_varint_encode_to_vec(buf: &mut [u8; 5], mut n: u32) -> usize {
    // 7 bits: leading 0, [7] value bits encoded directly
    if n < (1 << 7) {
        buf[0] = n as u8;
        return 1;
    }
    // 14 bits: leading 10, [6, 8] value bits encoded directly
    if n < (1 << 14) {
        buf[0] = 0b1000_0000 + (n & 0b0011_1111) as u8;
        n >>= 6;
        buf[1] = n as u8;
        return 2;
    }
    // 21 bits: leading 110, [5, 8, 8] value bits encoded directly
    if n < (1 << 21) {
        buf[0] = 0b1100_0000 + (n & 0b0001_1111) as u8;
        n >>= 5;
        buf[1] = n as u8;
        n >>= 8;
        buf[2] = n as u8;
        return 3;
    }
    // 28 bits: leading 1110, [4, 8, 8, 8] value bits encoded directly
    if n < (1 << 28) {
        buf[0] = 0b1110_0000 + (n & 0b0000_1111) as u8;
        n >>= 4;
        buf[1] = n as u8;
        n >>= 8;
        buf[2] = n as u8;
        n >>= 8;
        buf[3] = n as u8;
        return 4;
    }
    // 32 bits: leading 11110, [3, 8, 8, 8, 8] value bits encoded directly
    buf[0] = 0b1111_0000 + (n & 0b0000_0111) as u8;
    n >>= 3;
    buf[1] = n as u8;
    n >>= 8;
    buf[2] = n as u8;
    n >>= 8;
    buf[3] = n as u8;
    n >>= 8;
    buf[4] = n as u8;
    return 5;
}

#[cfg(test)]
mod test {
    use super::{decode_from_vec, encode_to_vec, encode_u32, prefix_varint_encode_to_vec};

    fn encode_helper(n: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_u32(n, &mut buf).unwrap();
        buf
    }

    #[test]
    fn encode_golden() {
        assert_eq!(encode_helper(0), vec![0]);
        assert_eq!(encode_helper(127), vec![127]);
        assert_eq!(encode_helper(250), vec![186, 3]); // 0b11111010 --> [prefix (10) + 6 lsb (111010), 2 msb (0000_0011)]
        assert_eq!(encode_helper(251), vec![187, 3]);
        assert_eq!(encode_helper(255), vec![191, 3]);
        assert_eq!(encode_helper(256), vec![128, 4]); // 0b1_0000_0000 --> [prefix (10) + 6 lsb (000000), 3 remainders (0b100)]
        assert_eq!(encode_helper(16384), vec![192, 0, 2]); // 0b10_0000_0000_0000 --> [prefix (110) + 5 lsb (00000), 8 middles (0), 2 leftovers (0b10)]
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

    #[test]
    fn u16_efficiency() {
        assert_eq!(
            (0..=u16::MAX)
                .map(|v| encode_to_vec(v as u32).len())
                .sum::<usize>(),
            196101,
        );
        // Efficiency of just directly using a u16 fixed width encoding
        assert_eq!((0x1_0000) * 2, 131072,);

        let mut buf = [0u8; 5];
        assert_eq!(
            (0..=u16::MAX)
                .map(|v| vu128::encode_u32(&mut buf, v as u32))
                .sum::<usize>(),
            180096,
        );
        assert_eq!(
            (0..=u16::MAX)
                .map(|v| prefix_varint_encode_to_vec(&mut buf, v as u32))
                .sum::<usize>(),
            180096,
        );
    }
}
