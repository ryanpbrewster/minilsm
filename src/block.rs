use std::io::{Cursor, Read, Write};

use anyhow::{bail, Ok};

use crate::{varint, ByteSlice};

const TARGET_BLOCK_SIZE: u32 = 4096;

pub struct CompressedBlock(Box<[u8]>);

#[derive(Default, PartialEq, Eq, Debug)]
pub struct Block {
    num_entries: u32,
    data: Vec<u8>,
    offsets: Vec<u8>, // encoded as fixed-width u32s
}

impl Block {
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
    fn compress(&self) -> anyhow::Result<CompressedBlock> {
        let mut encoder = zstd::Encoder::new(Vec::new(), 0)?;
        std::io::copy(&mut Cursor::new(&self.data), &mut encoder)?;
        std::io::copy(&mut Cursor::new(&self.offsets), &mut encoder)?;
        std::io::copy(
            &mut Cursor::new(self.num_entries.to_be_bytes()),
            &mut encoder,
        )?;
        Ok(CompressedBlock(encoder.finish()?.into_boxed_slice()))
    }

    pub fn decompress(compressed: CompressedBlock) -> anyhow::Result<Self> {
        let mut raw = zstd::decode_all(Cursor::new(compressed.0))?;
        let Some(num_entries_at) = raw.len().checked_sub(4) else {
            bail!("block is too short")
        };
        let num_entries = u32::from_be_bytes(raw.split_off(num_entries_at).as_slice().try_into()?);
        let Some(offsets_at) = raw.len().checked_sub(4 * num_entries as usize) else {
            bail!(
                "block is too short, num_elements={} but len={}",
                num_entries,
                raw.len()
            )
        };
        let offsets = raw.split_off(offsets_at);
        Ok(Block {
            num_entries,
            offsets,
            data: raw,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::ByteSlice;

    use super::Block;

    #[test]
    fn builder_smoke_test() -> anyhow::Result<()> {
        let mut block = Block::default();
        block.append(&ByteSlice::from_str(b"foo")?, &ByteSlice::from_str(b"bar")?)?;
        block.append(
            &ByteSlice::from_str(b"hello")?,
            &ByteSlice::from_str(b"world")?,
        )?;
        let compressed = block.compress()?;
        let decompressed = Block::decompress(compressed)?;
        assert_eq!(block, decompressed);
        Ok(())
    }
}
