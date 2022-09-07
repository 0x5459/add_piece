use std::io;
use std::mem;

use filecoin_hashers::{HashFunction, Hasher};
use filecoin_proofs::constants::DefaultPieceHasher;

use crate::commitment_reader::CommitmentReader;

pub struct ChunksReader<R: io::Read> {
    inner: CommitmentReader<R>,
    read_pos: usize,
    chunk_size: usize,
    chunk_roots: Vec<<DefaultPieceHasher as Hasher>::Domain>,
}

impl<R: io::Read> ChunksReader<R> {
    pub fn new(chunk_size_in_bytes: usize, inner: R) -> Self {
        let inner = CommitmentReader::new(inner);
        Self {
            inner,
            read_pos: 0,
            chunk_size: chunk_size_in_bytes,
            chunk_roots: Vec::new(),
        }
    }

    pub fn finish(self) -> <DefaultPieceHasher as Hasher>::Domain {
        let mut current_row = self.chunk_roots;

        while current_row.len() > 1 {
            let next_row = current_row
                .chunks(2)
                .map(|chunk| {
                    let buf = unsafe {
                        std::slice::from_raw_parts(
                            chunk.as_ptr() as *const u8,
                            mem::size_of::<<DefaultPieceHasher as Hasher>::Domain>() * 2,
                        )
                    };
                    <DefaultPieceHasher as Hasher>::Function::hash(buf)
                })
                .collect::<Vec<_>>();

            current_row = next_row;
        }
        debug_assert_eq!(current_row.len(), 1);

        current_row
            .into_iter()
            .next()
            .expect("should have been caught by debug build: len==1")
    }
}

impl<R: io::Read> io::Read for ChunksReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.read_pos >= self.chunk_size {
            self.read_pos = 0;
            self.chunk_roots.push(self.inner.compute());
            self.inner.reset();
        }

        let r = self.inner.read(buf)?;
        self.read_pos += r;
        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;
    use std::mem;

    use fr32::Fr32Reader;
    use storage_proofs_core::pieces::generate_piece_commitment_bytes_from_source;

    use filecoin_proofs::types::{PaddedBytesAmount, UnpaddedBytesAmount};

    #[test]
    fn test_commitment_reader() {
        const NODE_SIZE: usize = mem::size_of::<<DefaultPieceHasher as Hasher>::Domain>();

        let piece_size = 127 * 8;
        let source = vec![255u8; piece_size];
        let mut fr32_reader = Fr32Reader::new(Cursor::new(&source));

        let commitment1 = generate_piece_commitment_bytes_from_source::<DefaultPieceHasher>(
            &mut fr32_reader,
            PaddedBytesAmount::from(UnpaddedBytesAmount(piece_size as u64)).into(),
        )
        .expect("failed to generate piece commitment bytes from source");

        let fr32_reader = Fr32Reader::new(Cursor::new(&source));
        let mut chunks_reader = ChunksReader::new(NODE_SIZE * 4, fr32_reader);
        io::copy(&mut chunks_reader, &mut io::sink()).expect("io copy failed");

        let commitment2 = chunks_reader.finish();

        assert_eq!(&commitment1[..], AsRef::<[u8]>::as_ref(&commitment2));
    }
}
