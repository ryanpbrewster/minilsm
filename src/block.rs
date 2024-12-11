use std::io::{Cursor, Read, Write};

use anyhow::bail;

use crate::{varint, ByteSlice};

const TARGET_BLOCK_SIZE: u32 = 4096;

pub struct CompressedBlock(Box<[u8]>);

#[derive(Default)]
pub struct Builder {
    num_entries: u32,
    data: Vec<u8>,
    offsets: Vec<u8>, // encoded as fixed-width u32s
}

impl Builder {
    const TARGET_SIZE: usize = 4096;
    fn append(&mut self, key: &ByteSlice, value: &ByteSlice) -> anyhow::Result<()> {
        let cur_len = self.data.len() as u32;
        let encoded_len = varint::encoded_length_u32(key.len())
            + varint::encoded_length_u32(value.len())
            + key.len()
            + value.len();
        if cur_len > 0 && cur_len + encoded_len > TARGET_BLOCK_SIZE {
            bail!("would overflow non-empty block");
        }

        self.num_entries += 1;
        self.offsets.extend(cur_len.to_be_bytes());
        self.data.try_reserve(encoded_len as usize)?;
        varint::encode_u32(key.len(), &mut self.data)?;
        varint::encode_u32(value.len(), &mut self.data)?;
        self.data.write_all(key.as_ref())?;
        self.data.write_all(value.as_ref())?;

        Ok(())
    }
    fn finish(mut self) -> anyhow::Result<CompressedBlock> {
        self.offsets.extend(self.num_entries.to_be_bytes());

        let mut encoder = zstd::Encoder::new(Vec::new(), 0)?;
        std::io::copy(&mut Cursor::new(self.data), &mut encoder)?;
        std::io::copy(&mut Cursor::new(self.offsets), &mut encoder)?;
        Ok(CompressedBlock(encoder.finish()?.into_boxed_slice()))
    }
}

#[cfg(test)]
mod test {
    use crate::ByteSlice;

    use super::Builder;

    #[test]
    fn builder_smoke_test() -> anyhow::Result<()> {
        let mut b = Builder::default();
        b.append(&ByteSlice::from_str(b"foo")?, &ByteSlice::from_str(b"bar")?)?;
        b.append(
            &ByteSlice::from_str(b"hello")?,
            &ByteSlice::from_str(b"world")?,
        )?;
        b.finish()?;
        Ok(())
    }
}
