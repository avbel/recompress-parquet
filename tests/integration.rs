use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use recompress_parquet::{Codec, parse_compression, recompress};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn test_batch(schema: &Arc<Schema>) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
        ],
    )
    .unwrap()
}

fn create_test_parquet(path: &Path, compression: Compression) {
    let schema = test_schema();
    let batch = test_batch(&schema);

    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn read_all_batches(path: &Path) -> Vec<RecordBatch> {
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    reader.map(|r| r.unwrap()).collect()
}

fn get_compression(path: &Path) -> Compression {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let metadata = builder.metadata();
    metadata.row_group(0).column(0).compression()
}

// ---------------------------------------------------------------------------
// Roundtrip tests — one per codec
// ---------------------------------------------------------------------------

#[test]
fn test_recompress_to_zstd() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::SNAPPY);
    recompress(&input, &output, Compression::ZSTD(ZstdLevel::default())).unwrap();

    let src_batches = read_all_batches(&input);
    let dst_batches = read_all_batches(&output);
    assert_eq!(src_batches, dst_batches);
    assert!(matches!(get_compression(&output), Compression::ZSTD(_)));
}

#[test]
fn test_recompress_to_gzip() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::SNAPPY);
    recompress(&input, &output, Compression::GZIP(GzipLevel::default())).unwrap();

    let src_batches = read_all_batches(&input);
    let dst_batches = read_all_batches(&output);
    assert_eq!(src_batches, dst_batches);
    assert!(matches!(get_compression(&output), Compression::GZIP(_)));
}

#[test]
fn test_recompress_to_brotli() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::SNAPPY);
    recompress(&input, &output, Compression::BROTLI(BrotliLevel::default())).unwrap();

    let src_batches = read_all_batches(&input);
    let dst_batches = read_all_batches(&output);
    assert_eq!(src_batches, dst_batches);
    assert!(matches!(get_compression(&output), Compression::BROTLI(_)));
}

#[test]
fn test_recompress_to_snappy() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::GZIP(GzipLevel::default()));
    recompress(&input, &output, Compression::SNAPPY).unwrap();

    let src_batches = read_all_batches(&input);
    let dst_batches = read_all_batches(&output);
    assert_eq!(src_batches, dst_batches);
    assert_eq!(get_compression(&output), Compression::SNAPPY);
}

#[test]
fn test_recompress_to_lz4() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::SNAPPY);
    recompress(&input, &output, Compression::LZ4).unwrap();

    let src_batches = read_all_batches(&input);
    let dst_batches = read_all_batches(&output);
    assert_eq!(src_batches, dst_batches);
    assert_eq!(get_compression(&output), Compression::LZ4);
}

#[test]
fn test_recompress_to_lz4_raw() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::SNAPPY);
    recompress(&input, &output, Compression::LZ4_RAW).unwrap();

    let src_batches = read_all_batches(&input);
    let dst_batches = read_all_batches(&output);
    assert_eq!(src_batches, dst_batches);
    assert_eq!(get_compression(&output), Compression::LZ4_RAW);
}

#[test]
fn test_recompress_to_uncompressed() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::SNAPPY);
    recompress(&input, &output, Compression::UNCOMPRESSED).unwrap();

    let src_batches = read_all_batches(&input);
    let dst_batches = read_all_batches(&output);
    assert_eq!(src_batches, dst_batches);
    assert_eq!(get_compression(&output), Compression::UNCOMPRESSED);
}

// ---------------------------------------------------------------------------
// Compression with explicit levels
// ---------------------------------------------------------------------------

#[test]
fn test_recompress_gzip_level_9() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::SNAPPY);
    let compression = parse_compression(&Codec::Gzip, Some(9)).unwrap();
    recompress(&input, &output, compression).unwrap();

    assert_eq!(read_all_batches(&input), read_all_batches(&output));
    assert!(matches!(get_compression(&output), Compression::GZIP(_)));
}

#[test]
fn test_recompress_zstd_level_15() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::SNAPPY);
    let compression = parse_compression(&Codec::Zstd, Some(15)).unwrap();
    recompress(&input, &output, compression).unwrap();

    assert_eq!(read_all_batches(&input), read_all_batches(&output));
    assert!(matches!(get_compression(&output), Compression::ZSTD(_)));
}

#[test]
fn test_recompress_brotli_level_5() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::SNAPPY);
    let compression = parse_compression(&Codec::Brotli, Some(5)).unwrap();
    recompress(&input, &output, compression).unwrap();

    assert_eq!(read_all_batches(&input), read_all_batches(&output));
    assert!(matches!(get_compression(&output), Compression::BROTLI(_)));
}

// ---------------------------------------------------------------------------
// parse_compression — valid inputs
// ---------------------------------------------------------------------------

#[test]
fn test_parse_compression_all_codecs() {
    assert_eq!(
        parse_compression(&Codec::Uncompressed, None).unwrap(),
        Compression::UNCOMPRESSED
    );
    assert_eq!(
        parse_compression(&Codec::Snappy, None).unwrap(),
        Compression::SNAPPY
    );
    assert!(matches!(
        parse_compression(&Codec::Gzip, None).unwrap(),
        Compression::GZIP(_)
    ));
    assert!(matches!(
        parse_compression(&Codec::Brotli, None).unwrap(),
        Compression::BROTLI(_)
    ));
    assert!(matches!(
        parse_compression(&Codec::Zstd, None).unwrap(),
        Compression::ZSTD(_)
    ));
    assert_eq!(
        parse_compression(&Codec::Lz4, None).unwrap(),
        Compression::LZ4
    );
    assert_eq!(
        parse_compression(&Codec::Lz4raw, None).unwrap(),
        Compression::LZ4_RAW
    );
}

// ---------------------------------------------------------------------------
// parse_compression — invalid inputs
// ---------------------------------------------------------------------------

#[test]
fn test_parse_compression_level_on_unsupported_codec() {
    assert!(parse_compression(&Codec::Uncompressed, Some(1)).is_err());
    assert!(parse_compression(&Codec::Snappy, Some(1)).is_err());
    assert!(parse_compression(&Codec::Lz4, Some(1)).is_err());
    assert!(parse_compression(&Codec::Lz4raw, Some(1)).is_err());
}

#[test]
fn test_parse_compression_negative_level() {
    assert!(parse_compression(&Codec::Gzip, Some(-1)).is_err());
    assert!(parse_compression(&Codec::Brotli, Some(-1)).is_err());
}

#[test]
fn test_parse_compression_out_of_range_level() {
    assert!(parse_compression(&Codec::Gzip, Some(100)).is_err());
    assert!(parse_compression(&Codec::Brotli, Some(100)).is_err());
    assert!(parse_compression(&Codec::Zstd, Some(100)).is_err());
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[test]
fn test_recompress_nonexistent_input() {
    let dir = TempDir::new().unwrap();
    let result = recompress(
        Path::new("/nonexistent/file.parquet"),
        &dir.path().join("output.parquet"),
        Compression::SNAPPY,
    );
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Empty parquet
// ---------------------------------------------------------------------------

#[test]
fn test_recompress_empty_parquet() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("empty.parquet");
    let output = dir.path().join("output.parquet");

    // Create a parquet file with schema but zero rows.
    let schema = test_schema();
    let file = File::create(&input).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.close().unwrap();

    recompress(
        &input,
        &output,
        Compression::ZSTD(ZstdLevel::default()),
    )
    .unwrap();

    let batches = read_all_batches(&output);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

// ---------------------------------------------------------------------------
// Metadata preservation: key-value metadata
// ---------------------------------------------------------------------------

#[test]
fn test_preserves_key_value_metadata() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    // Create a parquet file with custom key-value metadata on the schema.
    let mut metadata = HashMap::new();
    metadata.insert("custom_key".to_string(), "custom_value".to_string());
    metadata.insert("another_key".to_string(), "another_value".to_string());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ],
        metadata.clone(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap();

    let file = File::create(&input).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    recompress(
        &input,
        &output,
        Compression::ZSTD(ZstdLevel::default()),
    )
    .unwrap();

    // Verify key-value metadata is preserved.
    let file = File::open(&output).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let output_metadata = builder.schema().metadata().clone();
    assert_eq!(output_metadata.get("custom_key").unwrap(), "custom_value");
    assert_eq!(output_metadata.get("another_key").unwrap(), "another_value");
}

// ---------------------------------------------------------------------------
// Metadata preservation: column statistics
// ---------------------------------------------------------------------------

#[test]
fn test_preserves_column_statistics() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    create_test_parquet(&input, Compression::SNAPPY);
    recompress(
        &input,
        &output,
        Compression::ZSTD(ZstdLevel::default()),
    )
    .unwrap();

    let file = File::open(&output).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let metadata = builder.metadata();
    let rg = metadata.row_group(0);

    // Both columns should have statistics.
    for col_idx in 0..rg.num_columns() {
        let col_meta = rg.column(col_idx);
        assert!(
            col_meta.statistics().is_some(),
            "column {col_idx} should have statistics"
        );
    }
}

// ---------------------------------------------------------------------------
// Metadata preservation: sorting columns
// ---------------------------------------------------------------------------

#[test]
fn test_preserves_sorting_columns() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    let schema = test_schema();

    // Create sorted data.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
        ],
    )
    .unwrap();

    let sorting_cols = vec![parquet::format::SortingColumn::new(0, false, false)];

    let file = File::create(&input).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_sorting_columns(Some(sorting_cols.clone()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    recompress(
        &input,
        &output,
        Compression::ZSTD(ZstdLevel::default()),
    )
    .unwrap();

    // Verify sorting columns metadata is preserved.
    let file = File::open(&output).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let metadata = builder.metadata();
    let rg = metadata.row_group(0);
    let output_sorting = rg.sorting_columns().expect("sorting columns should be set");
    assert_eq!(output_sorting.len(), 1);
    assert_eq!(output_sorting[0].column_idx, 0);
    assert!(!output_sorting[0].descending);
    assert!(!output_sorting[0].nulls_first);
}

// ---------------------------------------------------------------------------
// Metadata preservation: row group boundaries
// ---------------------------------------------------------------------------

#[test]
fn test_preserves_row_group_boundaries() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("input.parquet");
    let output = dir.path().join("output.parquet");

    let schema = test_schema();

    // Create a parquet file with 3 row groups (1 row each).
    let file = File::create(&input).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(1)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
        ],
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // Verify input has 3 row groups.
    let file = File::open(&input).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let input_num_rg = builder.metadata().num_row_groups();
    assert_eq!(input_num_rg, 3);

    recompress(
        &input,
        &output,
        Compression::ZSTD(ZstdLevel::default()),
    )
    .unwrap();

    // Verify output has the same number of row groups.
    let file = File::open(&output).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let output_num_rg = builder.metadata().num_row_groups();
    assert_eq!(output_num_rg, input_num_rg);

    // Verify data is identical.
    let src_batches = read_all_batches(&input);
    let dst_batches = read_all_batches(&output);
    assert_eq!(src_batches, dst_batches);
}
