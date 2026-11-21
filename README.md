# event-rollup — native Rust CSV aggregation

A single static Rust
binary reads the CSV inputs in `/data`, applies `rules.md`, and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet. No SQL engine.

## How it works

1. **Lookups.** `manifest.json`, `campaigns.csv`, `products.csv`. Categories are
   sorted byte-wise, so "smallest name wins a tie" is "smallest index wins".
2. **Links.** Built on two extra threads from process start, alongside the
   parse. All `links/*.csv` lines are parsed in parallel; their IDs are
   interned per hash shard with small tables of 32-bit slots, each key carrying
   the place of its id in the edge list. A union-find (union by size, path
   compression) joins them, and each person is named by the byte-wise smallest
   ID; IDs whose name differs from themselves go in a map.
3. **Parse.** Every CSV is memory-mapped and cut into chunks at `
` boundaries
   (only the first line of each file is skipped). A hand-written scanner follows
   the dialect exactly: only `
` ends a line, exact field count, quotes only
   around a whole field, no trimming, ASCII digits only. It works a line at a
   time on the newline/comma/quote bit masks of the 128 bytes at the line start
   (AVX-512 compares), checking the quote rules with mask arithmetic; long
   lines fall back to a scalar splitter. Timestamps F1/F2/F3 are validated fully
   (calendar, offsets, overflow). Each well-formed event line becomes a compact
   record (packed event_id, ingest_ts, a "valid and not late" flag, user, ts,
   price, category) scattered by a hash of event_id. Clicks are validated (ID,
   user, ts in the click window, known campaign). The pages of a chunk are
   released (`MADV_DONTNEED`) as soon as it is parsed, so the mappings are torn
   down in parallel rather than at exit.
4. **Dedup.** Per event_id partition, an open-addressing table of 32-bit slots
   (hash tag + record number) finds the copies of each event_id; the copy with
   the smallest ingest_ts is kept and the others are marked in a bitmap. The
   kept copy survives only if its flag is set. Survivors and clicks are mapped
   to their person and scattered by person.
5. **Sessions and attribution.** Per person partition, events are sorted by
   (person, ts) and clicks by (person, ts, click_id descending). One pass per
   person splits sessions on gaps over 30 minutes and slides a 7-day click
   window: the last click in the window is the credit, and a per-campaign
   count with an active set gives the distinct assisting campaigns.
   Per-thread accumulators collect user×day rows and a campaign×day array.
6. **Output.** Parquet files written in parallel, uncompressed, `day` as
   `INT32 (DATE)`.

IDs of up to 15 bytes are packed into a `u128` (bytes big-endian, length in the
low byte) so integer order is byte-wise string order; longer IDs fall back to an
interner. Timestamps are stored as `u32` milliseconds after `window_start - 7 days`.

## Libraries

- [rayon](https://crates.io/crates/rayon) — data parallelism
- [memmap2](https://crates.io/crates/memmap2) — memory-mapped input files
- [memchr](https://crates.io/crates/memchr) — fast byte search
- [mimalloc](https://crates.io/crates/mimalloc) — allocator

The Parquet files are written by a small hand-written writer (`src/out.rs`).

## Build and run

    docker buildx build --platform linux/amd64 --load -t event-rollup:latest .

Without Docker: `cargo build --release`, then
`DATA_DIR=... OUTPUT_DIR=... target/release/sessions`. Timings for each phase go
to stderr. Tuning knobs for testing (environment): `SS_CHUNK` (bytes per parse
chunk), `SS_PBITS` / `SS_QBITS` (log2 partition counts), `SS_THREADS`,
`SS_INLINE` (force IDs longer than N bytes onto the interned path), `SS_KEEP=1`
(do not release parsed input pages early), `SS_CHECK=1` (assert the table
invariants).

## Licence

MIT, see `LICENSE`.
