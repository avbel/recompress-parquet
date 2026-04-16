<p align="center">
  <img src="logo.png" width="300">
</p>

# recompress-parquet

A fast CLI tool to recompress [Apache Parquet](https://parquet.apache.org/) files with a different compression codec. Built with Rust for maximum performance.

## Installation

### From GitHub Releases

Download the latest binary for your platform from the [Releases page](https://github.com/avbel/recompress-parquet/releases).

| Platform | Archive |
|----------|---------|
| Linux x64 | `recompress-parquet-linux-x64.tar.gz` |
| macOS ARM64 | `recompress-parquet-macos-arm64.zip` |
| Windows x64 | `recompress-parquet-windows-x64.zip` |

### From Source

```sh
cargo install --path .
```

## Usage

```
recompress-parquet [OPTIONS] <INPUT> <OUTPUT>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<INPUT>` | Input parquet file path or glob pattern |
| `<OUTPUT>` | Output parquet file path or directory |

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `-c, --compression <CODEC>` | Compression codec (see table below) | `zstd` |
| `-l, --level <LEVEL>` | Compression level | codec default |
| `-V, --version` | Print version | |
| `-h, --help` | Print help | |

### Supported Codecs

| Codec | Level Range | Default Level |
|-------|-------------|---------------|
| `uncompressed` | N/A | N/A |
| `snappy` | N/A | N/A |
| `gzip` | 0-9 | 6 |
| `brotli` | 0-11 | 1 |
| `zstd` | 1-22 | 3 |
| `lz4` | N/A | N/A |
| `lz4raw` | N/A | N/A |

## Examples

### Basic recompression (defaults to zstd)

```sh
recompress-parquet input.parquet output.parquet
```

### Specify a codec

```sh
recompress-parquet -c gzip input.parquet output.parquet
```

### Specify codec and compression level

```sh
# High gzip compression
recompress-parquet -c gzip -l 9 input.parquet output.parquet

# Maximum zstd compression
recompress-parquet -c zstd -l 19 input.parquet output.parquet
```

### Decompress (store uncompressed)

```sh
recompress-parquet -c uncompressed input.parquet output.parquet
```

### Recompress all parquet files in a directory

When the input is a glob pattern matching multiple files, the output must be a directory:

```sh
recompress-parquet 'data/*.parquet' output_dir/
```

### Recompress with nested glob pattern

```sh
recompress-parquet 'data/**/*.parquet' recompressed/
```

### Switch from snappy to zstd for better compression ratio

```sh
recompress-parquet -c zstd -l 3 legacy_snappy.parquet optimized.parquet
```

## What is preserved

When recompressing, the tool preserves:

- Row group boundaries (same number of row groups, same row counts)
- Column statistics (min, max, null count)
- Key-value metadata (custom metadata on the schema)
- Sorting column information

## License

[MIT](LICENSE)
