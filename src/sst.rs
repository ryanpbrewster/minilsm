use std::io::Write;

use byteorder::WriteBytesExt;
use serde::{Deserialize, Serialize};

use crate::{block::Block, ByteSlice};

#[derive(Default, Serialize, Deserialize)]
struct Metadata {

}

struct Table {

}

struct TrackingWrite<W: Write> {
    inner: W,
    written: usize,
}
impl <W: Write> TrackingWrite<W> {
    fn new(inner: W) -> Self {
        Self { inner, written: 0 }
    }
}
impl <W: Write> Write for TrackingWrite<W> {
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
impl <W: Write> Writer<W> {
    fn new(inner: W) -> Self {
        Self {
            inner: TrackingWrite::new(inner),
            meta: Metadata::default(),
            cur: Block::default(),
        }
    }
    fn append(&mut self, key: &ByteSlice, value: &ByteSlice) -> anyhow::Result<()> {
        if self.cur.append(key, value).is_err() {
            self.cur.drain(&mut self.inner)?;
            self.cur.append(key, value)?;
        }
        Ok(())
    }
    fn finish(mut self) -> anyhow::Result<()> {
        if !self.cur.is_empty() {
            self.cur.drain(&mut self.inner)?;
        }
        let meta_offset = self.inner.written as u64;
        serde_cbor::to_writer(&mut self.inner, &self.meta)?;
        self.inner.write_all(&meta_offset.to_be_bytes())?;
        self.inner.flush()?;
        eprintln!("wrote {} bytes", self.inner.written);
        // writer will be dropped, which should hopefully take care of any cleanup
        Ok(())
    }
}

impl Table {
}

struct TableIter {

}

impl TableIter {
    fn next() -> Option<(ByteSlice, ByteSlice)> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::ByteSlice;

    use super::Writer;

    #[test]
    fn sst_writer_smoke() -> anyhow::Result<()> {
        let mut w = Writer::new(tempfile::tempfile()?);
        w.append(
            &ByteSlice::from_str(b"hello")?,
            &ByteSlice::from_str(b"world")?,
        )?;
        w.finish()?;
        Ok(())
    }
}