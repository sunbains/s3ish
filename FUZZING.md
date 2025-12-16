# Fuzz Testing

s3ish includes comprehensive fuzz testing using [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (libFuzzer) to discover edge cases, crashes, and security vulnerabilities.

## Overview

Fuzz testing feeds random/mutated inputs to functions to find bugs that traditional unit tests miss. Our fuzz targets cover critical parsing and encoding paths:

- **storage_backend**: Bucket/key validation and storage operations
- **erasure_coding**: Data encoding/decoding with parity shards
- **sigv4_parsing**: AWS SigV4 authentication header and query string parsing
- **xml_parsing**: S3 XML request/response parsing

## Quick Start

### Installation

```bash
cargo install cargo-fuzz
```

### Running Fuzz Tests

Run a specific fuzz target for a limited time:

```bash
# Run storage backend fuzzing for 60 seconds
cargo fuzz run storage_backend -- -max_total_time=60

# Run erasure coding fuzzing for 5 minutes
cargo fuzz run erasure_coding -- -max_total_time=300

# Run SigV4 parsing fuzzing
cargo fuzz run sigv4_parsing -- -max_total_time=60

# Run XML parsing fuzzing
cargo fuzz run xml_parsing -- -max_total_time=60
```

Run until a crash is found (or Ctrl+C):

```bash
cargo fuzz run storage_backend
```

### Build All Fuzz Targets

```bash
cargo fuzz build
```

### List Available Targets

```bash
cargo fuzz list
```

## Fuzz Targets

### 1. storage_backend

**Purpose**: Test storage operations with malformed bucket names, keys, and data

**What it tests**:
- Bucket name validation (length, characters, edge cases)
- Object key validation (special characters, Unicode, path traversal)
- Storage operations (put, get, delete, list) with malformed inputs
- Error handling for invalid operations

**Example usage**:
```bash
cargo fuzz run storage_backend -- -max_total_time=300 -workers=4
```

**Performance**: ~40-60 exec/s (async operations are slower)

### 2. erasure_coding

**Purpose**: Test erasure encoding/decoding with various configurations and data patterns

**What it tests**:
- Data block and parity block configurations (1-10 data blocks, 1-3 parity blocks)
- Variable block sizes
- Encode/decode roundtrip verification
- Single shard recovery (missing data or parity shards)
- Edge cases: empty data, single byte, large inputs

**Example usage**:
```bash
cargo fuzz run erasure_coding -- -max_total_time=60
```

**Performance**: ~12,000-15,000 exec/s (very fast, pure computation)

### 3. sigv4_parsing

**Purpose**: Test AWS Signature V4 parsing with malformed inputs

**What it tests**:
- Authorization header parsing (`AWS4-HMAC-SHA256 Credential=...`)
- Credential component extraction (access_key/date/region/service/aws4_request)
- Query string parameter parsing (pre-signed URLs)
- X-Amz-* parameter handling (Algorithm, Credential, Date, Expires, SignedHeaders, Signature)
- Bucket/key name validation
- URI encoding/decoding edge cases

**Example usage**:
```bash
cargo fuzz run sigv4_parsing -- -max_total_time=120
```

**Performance**: ~100,000-120,000 exec/s (very fast, string parsing)

### 4. xml_parsing

**Purpose**: Test S3 XML parsing with malformed XML structures

**What it tests**:
- CreateBucketConfiguration XML with LocationConstraint
- CompleteMultipartUpload XML with Part elements (PartNumber, ETag)
- Multi-object Delete XML with Key elements and Quiet flag
- Error XML structure (Code, Message, Resource, RequestId)
- ListBucketResult XML with Contents entries
- Malformed XML (unmatched tags, wrong nesting, truncated documents)

**Example usage**:
```bash
cargo fuzz run xml_parsing -- -max_total_time=120
```

**Performance**: ~90,000-100,000 exec/s (very fast, string parsing)

## Advanced Usage

### Continuous Fuzzing

For long-running fuzzing campaigns:

```bash
# Run for 24 hours with 8 workers
cargo fuzz run erasure_coding -- -max_total_time=86400 -workers=8 -jobs=8
```

### Reproducing Crashes

If a crash is found, cargo-fuzz saves the failing input:

```bash
# The crash file is saved in fuzz/artifacts/<target>/crash-<hash>
cargo fuzz run storage_backend fuzz/artifacts/storage_backend/crash-da39a3ee
```

### Corpus Management

Fuzz tests build a corpus of interesting inputs over time:

```bash
# View corpus files
ls fuzz/corpus/storage_backend/

# Run with existing corpus
cargo fuzz run storage_backend -- -runs=1000

# Minimize corpus (remove redundant test cases)
cargo fuzz cmin storage_backend
```

### Coverage Analysis

Check code coverage achieved by fuzzing:

```bash
cargo fuzz coverage storage_backend
```

## Integration with CI/CD

Add to your CI pipeline:

```yaml
# .github/workflows/fuzz.yml
name: Fuzz Testing
on:
  schedule:
    - cron: '0 0 * * *'  # Daily
  workflow_dispatch:

jobs:
  fuzz:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [storage_backend, erasure_coding, sigv4_parsing, xml_parsing]
    steps:
      - uses: actions/checkout@v3
      - uses: dtolnay/rust-toolchain@nightly
      - run: cargo install cargo-fuzz
      - run: cargo fuzz run ${{ matrix.target }} -- -max_total_time=300
```

## Best Practices

1. **Run Regularly**: Fuzz tests should run continuously or at least daily
2. **Use Multiple Workers**: Leverage multiple CPU cores with `-workers=N`
3. **Save Corpus**: Commit interesting corpus files to version control
4. **Fix Crashes Immediately**: Fuzzing-discovered bugs are real security issues
5. **Monitor Coverage**: Ensure fuzz tests reach the code paths you care about

## Performance Tips

- **Fast Targets** (sigv4_parsing, xml_parsing): Run with `-workers=N` to use all cores
- **Slow Targets** (storage_backend): Limited by async runtime overhead, use fewer workers
- **Maximize Runs**: For pure computation targets (erasure_coding), aim for millions of runs
- **Memory Usage**: Monitor RSS; large corpus + workers can consume significant RAM

## Results

As of the latest run:

| Target | Runs/Second | Code Coverage | Notable Findings |
|--------|-------------|---------------|------------------|
| storage_backend | ~50 | 2,509 lines | Async storage operations tested |
| erasure_coding | ~12,000 | 681 lines | Encode/decode roundtrip verified |
| sigv4_parsing | ~120,000 | - | Authorization parsing tested |
| xml_parsing | ~90,000 | - | S3 XML formats tested |

## Troubleshooting

### "Command not found: cargo-fuzz"

Install cargo-fuzz:
```bash
cargo install cargo-fuzz
```

### Slow Fuzzing Performance

- Reduce workers: `-workers=1`
- Profile the fuzz target to find bottlenecks
- Consider simplifying the test case

### Out of Memory

- Reduce corpus size: `cargo fuzz cmin <target>`
- Reduce workers: `-workers=1`
- Limit memory: `-rss_limit_mb=2048`

## Resources

- [cargo-fuzz documentation](https://rust-fuzz.github.io/book/cargo-fuzz.html)
- [libFuzzer documentation](https://llvm.org/docs/LibFuzzer.html)
- [Fuzzing best practices](https://rust-fuzz.github.io/book/)
