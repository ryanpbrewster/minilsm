pub mod block;
pub mod sst;
pub mod varint;

/// ByteSlice is a slice of bytes that is known to have a u32 length.
/// You cannot use this to represent more than 4 GB of data.
#[derive(Debug, PartialEq, Eq)]
pub struct ByteString(u32, Box<[u8]>);
impl ByteString {
    pub fn from_str(v: &[u8]) -> anyhow::Result<ByteString> {
        let len: u32 = v.len().try_into()?;
        Ok(ByteString(len, v.to_vec().into_boxed_slice()))
    }
    pub fn assume(v: Vec<u8>) -> ByteString {
        let len: u32 = v.len().try_into().unwrap();
        ByteString(len, v.into_boxed_slice())
    }
    pub fn from_vec(v: Vec<u8>) -> anyhow::Result<ByteString> {
        let len: u32 = v.len().try_into()?;
        Ok(ByteString(len, v.into_boxed_slice()))
    }
    pub fn len(&self) -> u32 {
        self.0
    }
}
impl AsRef<[u8]> for ByteString {
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
