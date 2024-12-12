use std::io::Write;

use byteorder::ReadBytesExt;

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
    5
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

pub fn decode_u32(mut r: impl ReadBytesExt) -> anyhow::Result<u32> {
    let first = r.read_u8()?;
    if first < 0b1000_0000 {
        return Ok(first as u32);
    }
    if first < 0b1100_0000 {
        return Ok((first & 0b0011_1111) as u32 | (r.read_u8()? as u32) << 6);
    }
    if first < 0b1110_0000 {
        return Ok((first & 0b0001_1111) as u32
            | (r.read_u8()? as u32) << 5
            | (r.read_u8()? as u32) << 13);
    }
    if first < 0b1111_0000 {
        return Ok((first & 0b0000_1111) as u32
            | (r.read_u8()? as u32) << 4
            | (r.read_u8()? as u32) << 12
            | (r.read_u8()? as u32) << 20);
    }
    Ok((first & 0b0000_0111) as u32
        | (r.read_u8()? as u32) << 3
        | (r.read_u8()? as u32) << 11
        | (r.read_u8()? as u32) << 19
        | (r.read_u8()? as u32) << 27)
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::varint::{decode_u32, encoded_length_u32};

    use super::encode_u32;

    fn encode_helper(n: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_u32(n, &mut buf).unwrap();
        buf
    }
    fn decode_helper(v: Vec<u8>) -> Option<u32> {
        decode_u32(Cursor::new(v)).ok()
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
        assert_eq!(decode_helper(vec![0]), Some(0));
        assert_eq!(decode_helper(vec![127]), Some(127));
        assert_eq!(decode_helper(vec![186, 3]), Some(250)); // 0b10111010
        assert_eq!(decode_helper(vec![191, 3]), Some(255));
        assert_eq!(decode_helper(vec![192, 0, 2]), Some(16384));

        // Some broken decodings
        assert_eq!(decode_helper(vec![128]), None);

        // Some weird but technically valid encodings
        assert_eq!(decode_helper(vec![128, 0]), Some(0));
    }

    #[test]
    fn u16_efficiency() {
        assert_eq!(
            (0..=u16::MAX)
                .map(|n| encoded_length_u32(n as u32) as usize)
                .sum::<usize>(),
            180096,
        );
        // Efficiency of just directly using a u16 fixed width encoding
        assert_eq!((0x1_0000) * 2, 131072,);

        let mut buf = [0u8; 5];
        assert_eq!(
            (0..=u16::MAX)
                .map(|n| vu128::encode_u32(&mut buf, n as u32))
                .sum::<usize>(),
            180096,
        );
    }
}
