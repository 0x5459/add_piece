use std::cmp::min;
use std::io::{self, Read};
use std::mem;

use filecoin_hashers::{HashFunction, Hasher};
use filecoin_proofs::constants::DefaultPieceHasher;
use rayon::prelude::{ParallelIterator, ParallelSlice};

type HashDomain = <DefaultPieceHasher as Hasher>::Domain;

/// Calculates comm-d of the data piped through to it.
/// Data must be bit padded and power of 2 bytes.
pub struct CommitmentReader<R> {
    source: R,
    buffer: [u8; 64],
    buffer_pos: usize,
    current_tree: Vec<HashDomain>,
}

impl<R: Read> CommitmentReader<R> {
    pub fn new(source: R) -> Self {
        CommitmentReader {
            source,
            buffer: [0u8; 64],
            buffer_pos: 0,
            current_tree: Vec::new(),
        }
    }

    /// Attempt to generate the next hash, but only if the buffers are full.
    fn try_hash(&mut self) {
        if self.buffer_pos < 63 {
            return;
        }

        // WARNING: keep in sync with DefaultPieceHasher and its .node impl
        let hash = <DefaultPieceHasher as Hasher>::Function::hash(&self.buffer);
        self.current_tree.push(hash);
        self.buffer_pos = 0;

        // TODO: reduce hashes when possible, instead of keeping them around.
    }

    pub fn compute(&self) -> HashDomain {
        // ensure!(self.buffer_pos == 0, "not enough inputs provided");

        fn compute_row(row: &Vec<HashDomain>) -> Vec<HashDomain> {
            row.par_chunks(2)
                .map(|chunk| {
                    let buf = unsafe {
                        std::slice::from_raw_parts(
                            chunk.as_ptr() as *const u8,
                            mem::size_of::<HashDomain>() * 2,
                        )
                    };
                    <DefaultPieceHasher as Hasher>::Function::hash(buf)
                })
                .collect::<Vec<_>>()
        }

        let mut current_row = compute_row(&self.current_tree);

        while current_row.len() > 1 {
            current_row = compute_row(&current_row);
        }

        debug_assert_eq!(current_row.len(), 1);

        current_row
            .pop()
            .expect("should have been caught by debug build: len==1")
    }

    pub fn reset(&mut self) {
        self.buffer_pos = 0;
        self.current_tree.clear();
    }
}

impl<R: Read> Read for CommitmentReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let start = self.buffer_pos;
        let left = 64 - self.buffer_pos;
        let end = start + min(left, buf.len());

        // fill the buffer as much as possible
        let r = self.source.read(&mut self.buffer[start..end])?;

        // write the data, we read
        buf[..r].copy_from_slice(&self.buffer[start..start + r]);

        self.buffer_pos += r;

        // try to hash
        self.try_hash();

        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;

    use fr32::Fr32Reader;
    use storage_proofs_core::pieces::generate_piece_commitment_bytes_from_source;

    use filecoin_proofs::types::{PaddedBytesAmount, UnpaddedBytesAmount};

    #[test]
    fn test_commitment_reader() {
        let piece_size = 127 * 8;
        let source = vec![255u8; piece_size];
        let mut fr32_reader = Fr32Reader::new(Cursor::new(&source));

        let commitment1 = generate_piece_commitment_bytes_from_source::<DefaultPieceHasher>(
            &mut fr32_reader,
            PaddedBytesAmount::from(UnpaddedBytesAmount(piece_size as u64)).into(),
        )
        .expect("failed to generate piece commitment bytes from source");

        let fr32_reader = Fr32Reader::new(Cursor::new(&source));
        let mut commitment_reader = CommitmentReader::new(fr32_reader);
        io::copy(&mut commitment_reader, &mut io::sink()).expect("io copy failed");

        let commitment2 = commitment_reader.compute();

        assert_eq!(&commitment1[..], AsRef::<[u8]>::as_ref(&commitment2));
    }
}
