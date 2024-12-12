use std::{
    fs::File,
    io::{Seek, Write},
    os::unix::fs::FileExt,
    sync::Arc,
};

use anyhow::bail;
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

use crate::{
    block::{Block, BlockIter, CompressedBlock, OwnedBlockIter},
    ByteString,
};

#[derive(Default, Serialize, Deserialize, Debug)]
struct Metadata {
    bookends: Vec<u64>,
}

struct TrackingWrite<W: Write> {
    inner: W,
    written: usize,
}
impl<W: Write> TrackingWrite<W> {
    fn new(inner: W) -> Self {
        Self { inner, written: 0 }
    }
}
impl<W: Write> Write for TrackingWrite<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.written += n;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
struct Writer<W: Write> {
    inner: TrackingWrite<W>,
    meta: Metadata,
    cur: Block,
}
impl<W: Write> Writer<W> {
    fn new(inner: W) -> Self {
        Self {
            inner: TrackingWrite::new(inner),
            meta: Metadata::default(),
            cur: Block::default(),
        }
    }
    fn append(&mut self, key: &ByteString, value: &ByteString) -> anyhow::Result<()> {
        if self.cur.append(key, value).is_err() {
            self.cur.drain(&mut self.inner)?;
            self.meta.bookends.push(self.inner.written as u64);
            self.cur.append(key, value)?;
        }
        Ok(())
    }
    fn finish(mut self) -> anyhow::Result<()> {
        if !self.cur.is_empty() {
            self.cur.drain(&mut self.inner)?;
            self.meta.bookends.push(self.inner.written as u64);
        }
        let meta_offset = self.inner.written as u64;
        serde_cbor::to_writer(&mut self.inner, &self.meta)?;
        eprintln!("writing sst meta: {:?}", self.meta);
        self.inner.write_all(&meta_offset.to_be_bytes())?;
        self.inner.flush()?;
        eprintln!("wrote {} bytes", self.inner.written);
        // writer will be dropped, which should hopefully take care of any cleanup
        Ok(())
    }
}

struct Table {
    meta: Metadata,
    reader: File,
}
impl Table {
    fn open(reader: File) -> anyhow::Result<Self> {
        let len = reader.metadata()?.len();
        let (meta_offset, meta_len) = {
            let Some(offset) = len.checked_sub(std::mem::size_of::<u64>() as u64) else {
                bail!("file not long enough: {len}")
            };
            let mut buf = [0u8; std::mem::size_of::<u64>()];
            reader.read_exact_at(&mut buf, offset)?;
            let start = u64::from_be_bytes(buf);
            let Some(len) = offset.checked_sub(start) else {
                bail!("invalid meta offset: {start}")
            };
            (start, len)
        };

        let mut meta_bytes = vec![0u8; meta_len as usize];
        reader.read_exact_at(meta_bytes.as_mut_slice(), meta_offset)?;
        let meta: Metadata = serde_cbor::from_slice(&meta_bytes)?;
        eprintln!("read metadata: {meta:?}");
        Ok(Self { reader, meta })
    }
    fn read_block(&self, idx: usize) -> anyhow::Result<Option<Block>> {
        if idx >= self.meta.bookends.len() {
            return Ok(None);
        }
        let start = if idx == 0 {
            0
        } else {
            self.meta.bookends[idx - 1]
        };
        let end = self.meta.bookends[idx];
        let mut buf = vec![0; (end - start) as usize];
        self.reader.read_exact_at(&mut buf, start)?;
        let block = Block::decompress(CompressedBlock(buf.into_boxed_slice()))?;
        Ok(Some(block))
    }
    fn iter<'a>(&'a self) -> TableIter<'a> {
        TableIter {
            table: self,
            cur: None,
            next_block: 0,
        }
    }
}

struct TableIter<'a> {
    table: &'a Table,
    cur: Option<OwnedBlockIter>,
    next_block: usize,
}

impl TableIter<'_> {
    fn next(&mut self) -> anyhow::Result<Option<(ByteString, ByteString)>> {
        if let Some(cur) = self.cur.as_mut() {
            if let Some(next) = cur.next()? {
                return Ok(Some(next));
            }
        }
        // If we're here we need to open the next block
        let Some(block) = self.table.read_block(self.next_block)? else {
            return Ok(None);
        };
        self.cur = Some(block.into_iter());
        self.next_block += 1;
        self.next()
    }
}

#[cfg(test)]
mod test {
    use crate::ByteString;

    use super::{Table, Writer};

    #[test]
    fn sst_writer_smoke() -> anyhow::Result<()> {
        let mut f = tempfile::tempfile()?;
        let (k1, v1) = (
            ByteString::from_str(b"hello")?,
            ByteString::from_str(b"world")?,
        );
        {
            let mut w = Writer::new(&mut f);
            w.append(&k1, &v1)?;
            w.finish()?;
        }

        let t = Table::open(f)?;
        let mut iter = t.iter();
        assert_eq!(iter.next()?, Some((k1, v1)));
        assert_eq!(iter.next()?, None);
        Ok(())
    }

    #[test]
    fn sst_writer_compression_efficiency() -> anyhow::Result<()> {
        let mut f = tempfile::tempfile()?;

        let mut w = Writer::new(&mut f);
        for i in 0..1_000_000 {
            let k = ByteString::from_vec(format!("key-{i:06}").into_bytes())?;
            let v = ByteString::from_vec(format!("value-{i:06}").into_bytes())?;
            w.append(&k, &v)?;
        }
        w.finish()?;
        assert_eq!(f.metadata()?.len(), 4_212_165); // ~4MB for 1M entries, each entry is ~30 bytes, so this is ~7x compression

        let t = Table::open(f)?;
        let mut iter = t.iter();
        let mut count = 0;
        while iter.next()?.is_some() {
            count += 1;
        }
        assert_eq!(count, 1_000_000);
        Ok(())
    }
}
