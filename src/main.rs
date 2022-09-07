use std::{
    fs,
    path::{Path, PathBuf},
};

use add_piece::write_and_preprocess;
use anyhow::{Context, Result};
use filecoin_proofs::{PieceInfo, UnpaddedBytesAmount};
use tracing::debug;
use vc_processors::{
    builtin::tasks::AddPieces,
    core::{ext::run_consumer, Processor, Task},
};

#[derive(Copy, Clone, Default, Debug)]
pub struct AddPiecesProcessor;

impl Processor<AddPieces> for AddPiecesProcessor {
    fn process(&self, task: AddPieces) -> Result<<AddPieces as Task>::Output> {
        let staged_file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            // to make sure that we won't write into the staged file with any data exists
            .truncate(true)
            .open(&task.staged_filepath)
            .with_context(|| format!("open staged file: {}", task.staged_filepath.display()))?;

        let mut piece_infos = Vec::with_capacity(task.pieces.len().min(1));
        for piece in task.pieces {
            debug!(piece_file = ?piece.piece_file, "trying to add piece");
            let source =
                piece::fetcher::open(piece.piece_file, piece.payload_size, piece.piece_size.0)
                    .context("open piece file")?;
            let (piece_info, _) =
                write_and_preprocess(task.seal_proof_type, source, &staged_file, piece.piece_size)
                    .context("add piece")?;
            piece_infos.push(piece_info);
        }

        if piece_infos.is_empty() {
            let sector_size: u64 = task.seal_proof_type.sector_size().into();

            let pi = piece::add_piece_for_cc_sector(&staged_file, sector_size)
                .context("add piece for cc sector")?;
            piece_infos.push(pi);
        }

        Ok(piece_infos)
    }
}

#[derive(clap::Parser)]
struct Args {
    #[clap(subcommand)]
    action: Action,
}

#[derive(clap::Subcommand)]
enum Action {
    Processor,
    Add {
        pieces: Vec<(PathBuf, usize)>,
        out: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.action {
        Action::Processor => processor(),
        Action::Add { pieces, out } => {
            println!("{:?}", add_pieces(&pieces, out)?);
            Ok(())
        }
    }
}

fn processor() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::DEBUG.into())
                .from_env()
                .context("env filter")?,
        )
        .init();

    info!("start add_pieces consumer");
    run_consumer::<AddPieces, AddPiecesProcessor>()
}

fn add_pieces(pieces: &Vec<(PathBuf, usize)>, out: AsRef<Path>) -> Result<Vec<PieceInfo>> {
    let target_file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        // to make sure that we won't write into the staged file with any data exists
        .truncate(true)
        .open(out.as_ref())
        .with_context(|| format!("open staged file: {}", out.as_ref().display()))?;

    let piece_infos = Vec::with_capacity(pieces.len());
    for (piece_path, piece_size) in pieces {
        let (piece_info, _) = add_piece::add_piece(
            fs::File::open(piece),
            target_file,
            UnpaddedBytesAmount(piece_size),
            Default::default(),
        )
        .context("add_piece")?;
        piece_infos.push(piece_info);
    }

    Ok(piece_infos)
}
