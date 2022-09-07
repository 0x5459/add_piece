use std::{
    fs,
    path::{Path, PathBuf},
};

use add_piece::write_and_preprocess;
use anyhow::{Context, Result};
use clap::{Arg, ArgAction, Command};
use filecoin_proofs::{PieceInfo, UnpaddedBytesAmount};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*, EnvFilter};
use vc_processors::{
    builtin::{processors::piece, tasks::AddPieces},
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

fn cli() -> Command<'static> {
    Command::new("add_pieces")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(Command::new("processor").about("run a vc-processor for add_pieces"))
        .subcommand(
            Command::new("add_pieces")
                .arg(
                    Arg::new("pieces_json")
                        .value_parser(clap::value_parser!(String))
                        .required(true),
                )
                .arg(
                    Arg::new("out")
                        .value_parser(clap::value_parser!(PathBuf))
                        .required(true),
                )
                .arg(Arg::new("origin").long("origin").action(ArgAction::SetTrue)),
        )
}

#[derive(Debug, Deserialize, Serialize)]
struct PieceFile {
    path: PathBuf,
    size: u64,
}

fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::DEBUG.into())
                .from_env()
                .context("env filter")?,
        )
        .init();

    let m = cli().get_matches();
    match m.subcommand() {
        Some(("processor", _)) => processor(),
        Some(("add_pieces", add_pieces_m)) => {
            let origin = add_pieces_m.get_flag("origin");
            info!("add_pieces for {}", if origin { "origin" } else { "new" });

            let pieces_json = add_pieces_m
                .get_one::<String>("pieces_json")
                .expect("validated by clap");
            let out = add_pieces_m
                .get_one::<PathBuf>("out")
                .expect("validated by clap");

            let pieces: Vec<PieceFile> =
                serde_json::from_str(pieces_json).context("parse pieces_json")?;

            let piece_infos = add_pieces(&pieces, out, origin)?;
            println!("{:?}", piece_infos);
            Ok(())
        }
        _ => unreachable!(),
    }
}

fn processor() -> Result<()> {
    info!("start add_pieces consumer");
    run_consumer::<AddPieces, AddPiecesProcessor>()
}

fn add_pieces(
    pieces: &Vec<PieceFile>,
    out: impl AsRef<Path>,
    origin: bool,
) -> Result<Vec<PieceInfo>> {
    let target_file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        // to make sure that we won't write into the staged file with any data exists
        .truncate(true)
        .open(out.as_ref())
        .with_context(|| format!("open staged file: {}", out.as_ref().display()))?;

    let mut piece_infos = Vec::with_capacity(pieces.len());
    for piece in pieces {
        let source = fs::File::open(&piece.path).context("open piece file")?;
        let piece_size = UnpaddedBytesAmount(piece.size);
        let (piece_info, _) = if origin {
            filecoin_proofs::write_and_preprocess(source, &target_file, piece_size)
                .context("write_and_preprocess")?
        } else {
            add_piece::add_piece(source, &target_file, piece_size, Default::default())
                .context("add_piece")?
        };
        piece_infos.push(piece_info);
    }

    Ok(piece_infos)
}
