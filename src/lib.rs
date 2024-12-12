pub mod block;
pub mod varint;
pub mod sst;

/// ByteSlice is a slice of bytes that is known to have a u32 length.
/// You cannot use this to represent more than 4 GB of data.
pub struct ByteSlice(u32, Box<[u8]>);
impl ByteSlice {
    pub fn from_str(v: &[u8]) -> anyhow::Result<ByteSlice> {
        let len: u32 = v.len().try_into()?;
        Ok(ByteSlice(len, v.to_vec().into_boxed_slice()))
    }
    pub fn from_vec(v: Vec<u8>) -> anyhow::Result<ByteSlice> {
        let len: u32 = v.len().try_into()?;
        Ok(ByteSlice(len, v.into_boxed_slice()))
    }
    pub fn len(&self) -> u32 {
        self.0
    }
}
impl AsRef<[u8]> for ByteSlice {
    fn as_ref(&self) -> &[u8] {
        &self.1
    }
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
