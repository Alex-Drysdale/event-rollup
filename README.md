# Shopping Sessions — native Rust pipeline

Builders Buildathon, Databricks "Shopping Sessions" track. A single static Rust
binary reads the CSV inputs in `/data`, applies `rules.md`, and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet. No SQL engine.

## How it works

1. **Lookups.** `manifest.json`, `campaigns.csv`, `products.csv`. Categories are
   sorted byte-wise, so "smallest name wins a tie" is "smallest index wins".
2. **Links.** All `links/*.csv` lines are parsed in parallel, then joined with a
   union-find (union by size, path compression). Each person is named by the
   byte-wise smallest ID; IDs whose name differs from themselves go in a map.
3. **Parse.** Every CSV is memory-mapped and cut into chunks at `\n` boundaries
   (only the first line of each file is skipped). A hand-written parser follows
   the dialect exactly: only `\n` ends a line, exact field count, quotes only
   around a whole field, no trimming, ASCII digits only. Timestamps F1/F2/F3 are
   validated fully (calendar, offsets, overflow). Each well-formed event line
   becomes a compact record (packed event_id, ingest_ts, a "valid and not late"
   flag, user, ts, price, category) scattered by a hash of event_id. Clicks are
   validated (ID, user, ts in the click window, known campaign) and scattered
   by a hash of their person.
4. **Dedup.** Per event_id partition, an open-addressing table keeps the copy
   with the smallest ingest_ts; the kept copy survives only if its flag is set.
   Survivors are mapped to their person and scattered by person.
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
- [parquet](https://crates.io/crates/parquet) (Apache Arrow Rust) — Parquet writer

## Build and run

    docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .

Without Docker: `cargo build --release`, then
`DATA_DIR=... OUTPUT_DIR=... target/release/sessions`. Timings for each phase go
to stderr. Tuning knobs for testing (environment): `SS_CHUNK` (bytes per parse
chunk), `SS_PBITS` / `SS_QBITS` (log2 partition counts), `SS_THREADS`,
`SS_INLINE` (force IDs longer than N bytes onto the interned path), `SS_CHECK=1`
(assert the table invariants).

## Licence

MIT, see `LICENSE`.
