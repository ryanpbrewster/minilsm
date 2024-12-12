use std::io::{Cursor, Read, Write};

use anyhow::{bail, Ok};
use byteorder::{BigEndian, ByteOrder};

use crate::{varint, ByteString};

pub struct CompressedBlock(pub Box<[u8]>);

#[derive(Default, PartialEq, Eq, Debug)]
pub struct Block {
    num_entries: u32,
    data: Vec<u8>,
    offsets: Vec<u8>, // encoded as fixed-width u32s
}

impl Block {
    const TARGET_SIZE: u32 = 1 << 16;
    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }
    pub fn len(&self) -> u32 {
        self.num_entries
    }
    pub fn clear(&mut self) {
        self.num_entries = 0;
        self.data.clear();
        self.offsets.clear();
    }
    pub fn append(&mut self, key: &ByteString, value: &ByteString) -> anyhow::Result<()> {
        let cur_len = self.data.len() as u32;
        let encoded_len = varint::encoded_length_u32(key.len())
            + varint::encoded_length_u32(value.len())
            + key.len()
            + value.len();
        if cur_len > 0 && cur_len + encoded_len > Block::TARGET_SIZE {
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
    /// write encodes this block's contents into the provided writer.
    /// The structure is based on https://skyzh.github.io/mini-lsm/week1-03-block.html
    /// [KV1, KV2, ...; OFF1, OFF2, ...; NUM_ENTRIES]
    /// KV is [vlq(key.length), vlq(value.length), key..., value...] where vlq is a variable length quantity
    /// OFF is a fixed-size u32
    /// NUM_ENTRIES is a fixed-size u32
    /// Today this uses zstd, in the future we may want to make that configurable.
    fn write(&self, w: impl Write) -> anyhow::Result<()> {
        let mut encoder = zstd::Encoder::new(w, 0)?;
        std::io::copy(&mut Cursor::new(&self.data), &mut encoder)?;
        std::io::copy(&mut Cursor::new(&self.offsets), &mut encoder)?;
        std::io::copy(
            &mut Cursor::new(self.num_entries.to_be_bytes()),
            &mut encoder,
        )?;
        encoder.finish()?;
        Ok(())
    }
    /// drain is a write + clear
    pub fn drain(&mut self, w: impl Write) -> anyhow::Result<()> {
        self.write(w)?;
        self.clear();
        Ok(())
    }
    pub fn compress(&self) -> anyhow::Result<CompressedBlock> {
        let mut buf = Vec::new();
        self.write(&mut buf)?;
        Ok(CompressedBlock(buf.into_boxed_slice()))
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

    pub fn iter<'a>(&'a self) -> BlockIter<'a> {
        BlockIter {
            underlying: self,
            i: 0,
        }
    }
    pub fn into_iter(self) -> OwnedBlockIter {
        OwnedBlockIter {
            underlying: self,
            i: 0,
        }
    }
    fn fetch_entry(&self, i: u32) -> anyhow::Result<Option<(ByteString, ByteString)>> {
        if i >= self.num_entries {
            return Ok(None);
        }
        let off = u32::from_be_bytes(self.offsets[4 * i as usize..][..4].try_into()?);
        let mut cursor = Cursor::new(&self.data[off as usize..]);
        let key_len = varint::decode_u32(&mut cursor)?;
        let value_len = varint::decode_u32(&mut cursor)?;
        let mut key = vec![0; key_len as usize];
        cursor.read_exact(&mut key)?;
        let mut value = vec![0; value_len as usize];
        cursor.read_exact(&mut value)?;
        Ok(Some((ByteString::assume(key), ByteString::assume(value))))
    }
}

pub struct BlockIter<'a> {
    underlying: &'a Block,
    i: u32,
}
impl<'a> BlockIter<'a> {
    pub fn next(&mut self) -> anyhow::Result<Option<(ByteString, ByteString)>> {
        if self.i >= self.underlying.num_entries {
            return Ok(None);
        }
        let cur = self.i;
        self.i += 1;
        self.underlying.fetch_entry(cur)
    }
}

pub struct OwnedBlockIter {
    underlying: Block,
    i: u32,
}
impl OwnedBlockIter {
    pub fn next(&mut self) -> anyhow::Result<Option<(ByteString, ByteString)>> {
        if self.i >= self.underlying.num_entries {
            return Ok(None);
        }
        let cur = self.i;
        self.i += 1;
        self.underlying.fetch_entry(cur)
    }
}

#[cfg(test)]
mod test {
    use crate::ByteString;

    use super::Block;

    #[test]
    fn builder_smoke_test() -> anyhow::Result<()> {
        let mut block = Block::default();
        block.append(
            &ByteString::from_str(b"foo")?,
            &ByteString::from_str(b"bar")?,
        )?;
        block.append(
            &ByteString::from_str(b"hello")?,
            &ByteString::from_str(b"world")?,
        )?;
        let compressed = block.compress()?;
        let decompressed = Block::decompress(compressed)?;
        assert_eq!(block, decompressed);
        Ok(())
    }

    #[test]
    fn iter_smoke_test() -> anyhow::Result<()> {
        let mut block = Block::default();

        let (k1, v1) = (ByteString::from_str(b"foo")?, ByteString::from_str(b"bar")?);
        let (k2, v2) = (
            ByteString::from_str(b"hello")?,
            ByteString::from_str(b"world")?,
        );

        block.append(&k1, &v1)?;
        block.append(&k2, &v2)?;

        let mut iter = block.iter();
        assert_eq!(iter.next()?, Some((k1, v1)));
        assert_eq!(iter.next()?, Some((k2, v2)));
        assert_eq!(iter.next()?, None);
        assert_eq!(iter.next()?, None);
        Ok(())
    }
}
