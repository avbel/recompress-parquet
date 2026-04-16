use std::fs::File;
use std::path::Path;

use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum Codec {
    Uncompressed,
    Snappy,
    Gzip,
    Brotli,
    Lz4,
    Lz4raw,
    Zstd,
}

/// Convert a [`Codec`] and optional level into a parquet [`Compression`] value.
pub fn parse_compression(codec: &Codec, level: Option<i32>) -> Result<Compression, String> {
    match codec {
        Codec::Uncompressed => {
            if level.is_some() {
                return Err("compression level is not supported for uncompressed".into());
            }
            Ok(Compression::UNCOMPRESSED)
        }
        Codec::Snappy => {
            if level.is_some() {
                return Err("compression level is not supported for snappy".into());
            }
            Ok(Compression::SNAPPY)
        }
        Codec::Gzip => {
            let gzip_level = match level {
                Some(l) if l < 0 => {
                    return Err("gzip level must be non-negative (valid: 0-9)".into());
                }
                Some(l) => GzipLevel::try_new(l as u32)
                    .map_err(|e| format!("invalid gzip level (valid: 0-9): {e}"))?,
                None => GzipLevel::default(),
            };
            Ok(Compression::GZIP(gzip_level))
        }
        Codec::Brotli => {
            let brotli_level = match level {
                Some(l) if l < 0 => {
                    return Err("brotli level must be non-negative (valid: 0-11)".into());
                }
                Some(l) => BrotliLevel::try_new(l as u32)
                    .map_err(|e| format!("invalid brotli level (valid: 0-11): {e}"))?,
                None => BrotliLevel::default(),
            };
            Ok(Compression::BROTLI(brotli_level))
        }
        Codec::Zstd => {
            let zstd_level = match level {
                Some(l) => ZstdLevel::try_new(l)
                    .map_err(|e| format!("invalid zstd level (valid: 1-22): {e}"))?,
                None => ZstdLevel::default(),
            };
            Ok(Compression::ZSTD(zstd_level))
        }
        Codec::Lz4 => {
            if level.is_some() {
                return Err("compression level is not supported for lz4".into());
            }
            Ok(Compression::LZ4)
        }
        Codec::Lz4raw => {
            if level.is_some() {
                return Err("compression level is not supported for lz4raw".into());
            }
            Ok(Compression::LZ4_RAW)
        }
    }
}

/// Read a parquet file and write a copy with the specified compression.
///
/// Preserves key-value metadata, column statistics, sorting columns,
/// and row group boundaries from the source file.
pub fn recompress(
    input: &Path,
    output: &Path,
    compression: Compression,
) -> Result<(), Box<dyn std::error::Error>> {
    let input_file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(input_file)?;

    let schema = builder.schema().clone();
    let metadata = builder.metadata().clone();
    let num_row_groups = metadata.num_row_groups();

    // Extract sorting columns from the first row group (if present).
    let sorting_columns = if num_row_groups > 0 {
        metadata.row_group(0).sorting_columns().cloned()
    } else {
        None
    };

    let output_file = File::create(output)?;

    let mut props_builder = WriterProperties::builder()
        .set_compression(compression)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_created_by(format!(
            "recompress-parquet v{}",
            env!("CARGO_PKG_VERSION")
        ));

    if let Some(sorting_cols) = sorting_columns {
        props_builder = props_builder.set_sorting_columns(Some(sorting_cols));
    }

    let props = props_builder.build();

    let mut writer = ArrowWriter::try_new(output_file, schema.clone(), Some(props))?;

    // Process each row group separately to preserve row group boundaries.
    for rg_idx in 0..num_row_groups {
        let input_file = File::open(input)?;
        let rg_builder = ParquetRecordBatchReaderBuilder::try_new(input_file)?
            .with_row_groups(vec![rg_idx]);
        let rg_reader = rg_builder.build()?;

        for batch_result in rg_reader {
            let batch = batch_result?;
            writer.write(&batch)?;
        }

        writer.flush()?;
    }

    writer.close()?;
    Ok(())
}
