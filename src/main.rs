use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use parquet::basic::Compression;
use recompress_parquet::{Codec, parse_compression, recompress};

#[derive(Parser)]
#[command(version, about = "Recompress parquet files with a different compression codec")]
struct Cli {
    /// Input parquet file path or glob pattern
    input: String,

    /// Output parquet file path or directory (directory when input matches multiple files)
    output: PathBuf,

    /// Compression codec
    #[arg(short, long, value_enum, default_value_t = Codec::Zstd)]
    compression: Codec,

    /// Compression level (for gzip: 0-9, brotli: 0-11, zstd: 1-22)
    #[arg(short, long)]
    level: Option<i32>,
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let compression = parse_compression(&cli.compression, cli.level)?;

    let paths: Vec<PathBuf> = glob::glob(&cli.input)
        .map_err(|e| format!("invalid glob pattern: {e}"))?
        .filter_map(|entry| match entry {
            Ok(path) if path.is_file() => Some(path),
            Ok(_) => None,
            Err(e) => {
                eprintln!("Warning: {e}");
                None
            }
        })
        .collect();

    if paths.is_empty() {
        return Err(format!("no files matched '{}'", cli.input).into());
    }

    let multiple = paths.len() > 1;

    if multiple && cli.output.exists() && !cli.output.is_dir() {
        return Err("output must be a directory when input matches multiple files".into());
    }

    if multiple || (paths.len() == 1 && cli.output.is_dir()) {
        // Output is a directory
        std::fs::create_dir_all(&cli.output)?;

        for path in &paths {
            let file_name = path
                .file_name()
                .ok_or_else(|| format!("cannot determine file name for {}", path.display()))?;
            let out_path = cli.output.join(file_name);
            recompress_and_report(path, &out_path, compression)?;
        }
    } else {
        // Single file, output is a file path
        let path = &paths[0];
        if let Some(parent) = cli.output.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        recompress_and_report(path, &cli.output, compression)?;
    }

    println!("Done ({} file(s) processed)", paths.len());
    Ok(())
}

fn recompress_and_report(
    input: &Path,
    output: &Path,
    compression: Compression,
) -> Result<(), Box<dyn std::error::Error>> {
    let input_size = std::fs::metadata(input)?.len();
    println!("Recompressing {} -> {}", input.display(), output.display());
    recompress(input, output, compression)?;
    let output_size = std::fs::metadata(output)?.len();
    if input_size > 0 {
        let ratio = output_size as f64 / input_size as f64 * 100.0;
        println!("  {input_size} -> {output_size} bytes ({ratio:.0}% of original)");
    }
    Ok(())
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        eprintln!("Error: {e}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
