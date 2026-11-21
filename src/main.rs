//! Event pipeline. See README.md for the design.

mod csv;
mod keys;
mod out;
mod scan;
mod scatter;
mod table;
mod ts;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Instant;

use memmap2::Mmap;
use rayon::prelude::*;

use keys::{Key, KeyMap};
use table::{Interner, ShardedMap};
use ts::DAY_MS;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const GAP: u32 = 1_800_000;
const REACH: u32 = 604_800_000;
const LATE: i64 = 86_400_000;
const PRICE_MAX: u64 = 10_000_000;
const NO_CAT: u16 = u16::MAX;
const BATCH: usize = 16;

// ---------------------------------------------------------------------------
// Records

#[inline(always)]
fn split_key(k: Key) -> (u64, u64) {
    (k as u64, (k >> 64) as u64)
}

#[inline(always)]
fn join_key(lo: u64, hi: u64) -> Key {
    (lo as u128) | ((hi as u128) << 64)
}

/// A well-formed event line with a non-empty event_id, before dedup, in 48
/// bytes. `meta` packs ingest_ts (49 bits, milliseconds after year 0, so
/// order is preserved; unparseable sorts last), the category (14 bits) and
/// `ok` (1 bit): the line is valid and not late, so it survives if it is the
/// kept copy.
#[derive(Clone, Copy)]
struct ERec {
    eid_lo: u64,
    eid_hi: u64,
    user_lo: u64,
    user_hi: u64,
    meta: u64,
    ts: u32,
    price: u32,
}

const INGEST_BITS: u32 = 49;
const INGEST_MAX: u64 = (1 << INGEST_BITS) - 1;
/// Milliseconds from 0000-01-01 to the epoch.
const INGEST_OFFSET: i64 = 62_167_219_200_000;
const CAT_BITS: u32 = 14;
const CAT_NONE: u64 = (1 << CAT_BITS) - 1;

impl ERec {
    #[inline(always)]
    fn eid(&self) -> Key {
        join_key(self.eid_lo, self.eid_hi)
    }
    #[inline(always)]
    fn ingest_code(&self) -> u64 {
        self.meta >> (CAT_BITS + 1)
    }
    #[inline(always)]
    fn ok(&self) -> bool {
        self.meta & 1 != 0
    }
    #[inline(always)]
    fn cat(&self) -> u16 {
        let c = (self.meta >> 1) & CAT_NONE;
        if c == CAT_NONE { NO_CAT } else { c as u16 }
    }
}

#[inline(always)]
fn ingest_code(ingest: Option<i64>) -> u64 {
    match ingest {
        Some(t) => (t.saturating_add(INGEST_OFFSET)).clamp(0, INGEST_MAX as i64 - 1) as u64,
        None => INGEST_MAX,
    }
}

/// A valid click, before its user is mapped to a person.
#[derive(Clone, Copy)]
struct CRaw {
    c_lo: u64,
    c_hi: u64,
    u_lo: u64,
    u_hi: u64,
    ts: u32,
    camp: u32,
}

/// A surviving event. `key` is the person's hash in the high bits and ts in
/// the low `ts_bits`, so sorting by it groups by person and orders by time.
#[derive(Clone, Copy)]
struct PRec {
    key: u64,
    p_lo: u64,
    p_hi: u64,
    price: u32,
    cat: u16,
}

impl PRec {
    #[inline(always)]
    fn person(&self) -> Key {
        join_key(self.p_lo, self.p_hi)
    }
}

/// A valid click keyed like `PRec`.
#[derive(Clone, Copy)]
struct CRec {
    key: u64,
    p_lo: u64,
    p_hi: u64,
    c_lo: u64,
    c_hi: u64,
    camp: u32,
}

impl CRec {
    #[inline(always)]
    fn person(&self) -> Key {
        join_key(self.p_lo, self.p_hi)
    }
    #[inline(always)]
    fn cid(&self) -> Key {
        join_key(self.c_lo, self.c_hi)
    }
}

struct UserRow {
    person: Key,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: u16,
}

// ---------------------------------------------------------------------------
// Config and context

struct Cfg {
    data: PathBuf,
    out: PathBuf,
    threads: usize,
    chunk: usize,
    pbits: u32,
    qbits: u32,
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

impl Cfg {
    fn load() -> Cfg {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let data = std::env::var("DATA_DIR")
            .ok()
            .or_else(|| args.first().cloned())
            .unwrap_or_else(|| "/data".into());
        let out = std::env::var("OUTPUT_DIR")
            .ok()
            .or_else(|| args.get(1).cloned())
            .unwrap_or_else(|| "/output".into());
        let threads = env_usize("NCPU")
            .filter(|&n| n > 0)
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()));
        if let Some(n) = env_usize("SS_INLINE") {
            keys::set_inline_max(n);
        }
        Cfg {
            data: data.into(),
            out: out.into(),
            threads: env_usize("SS_THREADS").unwrap_or(threads).max(1),
            chunk: env_usize("SS_CHUNK").unwrap_or(4 << 20).max(1),
            pbits: env_usize("SS_PBITS").unwrap_or(10) as u32,
            qbits: env_usize("SS_QBITS").unwrap_or(9) as u32,
        }
    }
}

struct Ctx {
    ws: i64,
    we: i64,
    /// `ws - 7 days`: every stored ts is milliseconds after this.
    base: i64,
    day0: i64,
    ndays: usize,
    ts_bits: u32,
    camp_map: KeyMap<u32>,
    camp_ids: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
    prod_map: KeyMap<u16>,
    cats: Vec<Vec<u8>>,
    unknown_cat: u16,
    pbits: u32,
    qbits: u32,
}

impl Ctx {
    #[inline(always)]
    fn ncamp(&self) -> usize {
        self.camp_ids.len()
    }
    #[inline(always)]
    fn ppart(&self, k: Key) -> usize {
        if self.pbits == 0 { 0 } else { (keys::hash(k) >> (64 - self.pbits)) as usize }
    }
    /// The person partition and the sort key's high bits, from one hash.
    /// The partition uses bits below the key's, so they stay independent.
    #[inline(always)]
    fn qpart_key(&self, person: Key, ts: u32) -> (usize, u64) {
        let h = keys::hash(person);
        let q = if self.qbits == 0 { 0 } else { ((h >> 7) & ((1u64 << self.qbits) - 1)) as usize };
        ((q), (h >> self.ts_bits << self.ts_bits) | ts as u64)
    }
    #[inline(always)]
    fn ts_of(&self, key: u64) -> u32 {
        (key & ((1u64 << self.ts_bits) - 1)) as u32
    }
}

macro_rules! log {
    ($t0:expr, $($arg:tt)*) => {
        eprintln!("[{:>8.3}s] {}", $t0.elapsed().as_secs_f64(), format!($($arg)*));
    };
}

/// Runs `jobs` jobs on `n` workers that each own one accumulator; each
/// worker calls `finish(acc, worker index)` when no jobs are left.
fn workers_then<A: Send>(
    n: usize,
    jobs: usize,
    init: impl Fn() -> A + Sync,
    work: impl Fn(&mut A, usize) + Sync,
    finish: impl Fn(&mut A, usize) + Sync,
) -> Vec<A> {
    let next = AtomicUsize::new(0);
    let mut out: Vec<Option<A>> = (0..n).map(|_| None).collect();
    rayon::scope(|s| {
        for (w, slot) in out.iter_mut().enumerate() {
            let (next, init, work, finish) = (&next, &init, &work, &finish);
            s.spawn(move |_| {
                let mut a = init();
                loop {
                    let j = next.fetch_add(1, AtomicOrdering::Relaxed);
                    if j >= jobs {
                        break;
                    }
                    work(&mut a, j);
                }
                finish(&mut a, w);
                *slot = Some(a);
            });
        }
    });
    out.into_iter().map(|a| a.unwrap()).collect()
}

// ---------------------------------------------------------------------------
// Inputs

fn read_manifest(path: &Path) -> (i64, i64) {
    let text = fs::read(path).expect("manifest.json");
    let find = |name: &str| -> i64 {
        let pat = format!("\"{name}\"");
        let at = text.windows(pat.len()).position(|w| w == pat.as_bytes()).expect("manifest key") + pat.len();
        let rest = &text[at..];
        let colon = rest.iter().position(|&c| c == b':').expect("manifest colon");
        let rest = &rest[colon + 1..];
        let q1 = rest.iter().position(|&c| c == b'"').expect("manifest value");
        let rest = &rest[q1 + 1..];
        let q2 = rest.iter().position(|&c| c == b'"').expect("manifest value end");
        ts::parse(&rest[..q2]).expect("manifest timestamp")
    };
    (find("window_start"), find("window_end"))
}

fn map_file(path: &Path) -> Option<Mmap> {
    let f = fs::File::open(path).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let m = unsafe { Mmap::map(&f).ok()? };
    // Ask the kernel to read the whole file ahead now, in case it is not cached.
    if std::env::var_os("SS_NOWILLNEED").is_none() {
        let _ = m.advise(memmap2::Advice::WillNeed);
    }
    Some(m)
}

/// Every `*.csv` in a folder, mapped (empty files skipped).
fn map_dir(dir: &Path) -> Vec<Mmap> {
    let Ok(rd) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = rd
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.ends_with(".csv") && !name.starts_with('.') && e.path().is_file()
        })
        .map(|e| e.path())
        .collect();
    paths.sort();
    paths.iter().filter_map(|p| map_file(p)).collect()
}

fn load_campaigns(path: &Path) -> (KeyMap<u32>, Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut map = KeyMap::default();
    let mut ids = Vec::new();
    let mut channels = Vec::new();
    if let Some(m) = map_file(path) {
        for (s, e) in csv::chunks(&m, usize::MAX / 2) {
            csv::for_lines(&m[s..e], |line| {
                if let Some([id, ch]) = csv::split::<2>(line) {
                    let k = keys::pack(id);
                    if !map.contains_key(&k) {
                        map.insert(k, ids.len() as u32);
                        ids.push(id.to_vec());
                        channels.push(ch.to_vec());
                    }
                }
            });
        }
    }
    (map, ids, channels)
}

fn load_products(path: &Path) -> (KeyMap<u16>, Vec<Vec<u8>>, u16) {
    let mut rows: Vec<(Key, Vec<u8>)> = Vec::new();
    if let Some(m) = map_file(path) {
        for (s, e) in csv::chunks(&m, usize::MAX / 2) {
            csv::for_lines(&m[s..e], |line| {
                if let Some([id, cat]) = csv::split::<2>(line) {
                    rows.push((keys::pack(id), cat.to_vec()));
                }
            });
        }
    }
    let mut cats: Vec<Vec<u8>> = rows.iter().map(|r| r.1.clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    assert!(cats.len() < CAT_NONE as usize, "too many categories");
    let idx = |c: &[u8]| cats.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u16;
    let mut map = KeyMap::default();
    for (k, c) in &rows {
        map.entry(*k).or_insert_with(|| idx(c));
    }
    let unknown = idx(b"unknown");
    (map, cats, unknown)
}

/// Union-find over links/: a map from every linked ID whose person name
/// differs from itself to that name (the byte-wise smallest ID of the person).
fn build_links(files: &[Mmap], chunk: usize) -> ShardedMap {
    let t0 = Instant::now();
    let jobs: Vec<(usize, usize, usize)> = files
        .iter()
        .enumerate()
        .flat_map(|(fi, m)| csv::chunks(m, chunk).into_iter().map(move |(s, e)| (fi, s, e)))
        .collect();
    // Per job: the pairs, and their keys bucketed by shard.
    let parsed: Vec<(Vec<(Key, Key)>, Vec<Vec<Key>>)> = jobs
        .par_iter()
        .map(|&(fi, s, e)| {
            let file = &files[fi][..];
            let mut pairs = Vec::with_capacity((e - s) / 16);
            let mut by_shard: Vec<Vec<Key>> = (0..table::SHARDS).map(|_| Vec::new()).collect();
            scan::scan::<2>(file, s, e, |f| {
                let [(as_, ae), (bs, be)] = *f;
                if as_ != ae && bs != be {
                    let a = scan::pack_at(file, as_, ae);
                    let b = scan::pack_at(file, bs, be);
                    if a != b {
                        pairs.push((a, b));
                        by_shard[table::shard_of(keys::hash(a))].push(a);
                        by_shard[table::shard_of(keys::hash(b))].push(b);
                    }
                }
            });
            (pairs, by_shard)
        })
        .collect();
    let lists: Vec<Vec<Key>> = (0..table::SHARDS)
        .into_par_iter()
        .map(|sh| {
            let mut v = Vec::with_capacity(parsed.iter().map(|p| p.1[sh].len()).sum());
            for p in &parsed {
                v.extend_from_slice(&p.1[sh]);
            }
            v
        })
        .collect();
    let interner = Interner::build(&lists);
    let n = interner.keys.len();
    let edges: Vec<Vec<(u32, u32)>> = parsed
        .par_iter()
        .map(|(pairs, _)| pairs.iter().map(|&(a, b)| (interner.id(a), interner.id(b))).collect())
        .collect();
    log!(t0, "links interned: {n} ids");

    let mut parent: Vec<u32> = (0..n as u32).collect();
    let mut size: Vec<u32> = vec![1; n];
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        let mut root = x;
        while parent[root as usize] != root {
            root = parent[root as usize];
        }
        while parent[x as usize] != root {
            let next = parent[x as usize];
            parent[x as usize] = root;
            x = next;
        }
        root
    }
    for v in &edges {
        for &(ia, ib) in v {
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                let (big, small) = if size[ra as usize] >= size[rb as usize] { (ra, rb) } else { (rb, ra) };
                parent[small as usize] = big;
                size[big as usize] += size[small as usize];
            }
        }
    }
    let ids = &interner.keys;
    let mut name: Vec<Key> = vec![0; n];
    let mut has = vec![false; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if !has[r] || keys::cmp(ids[i], name[r]) == std::cmp::Ordering::Less {
            name[r] = ids[i];
            has[r] = true;
        }
    }
    log!(t0, "links joined");
    // Every parent is now a root.
    let renamed: Vec<Vec<(Key, Key)>> = (0..table::SHARDS)
        .into_par_iter()
        .map(|sh| {
            (interner.offsets[sh]..interner.offsets[sh + 1])
                .filter_map(|i| {
                    let nm = name[parent[i] as usize];
                    (nm != ids[i]).then_some((ids[i], nm))
                })
                .collect()
        })
        .collect();
    let map = ShardedMap::build(&renamed);
    log!(t0, "links table: {} renamed", renamed.iter().map(|v| v.len()).sum::<usize>());
    map
}

// ---------------------------------------------------------------------------
// Parse pass

#[inline(always)]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > PRICE_MAX {
            return None;
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}

#[derive(Clone, Copy)]
enum Kind {
    Events,
    Clicks,
}

struct A1 {
    ev: scatter::Scatter<ERec>,
    ck: Vec<CRaw>,
}

#[inline(never)]
fn parse_events(file: &[u8], s: usize, e: usize, ctx: &Ctx, acc: &mut A1) {
    scan::scan::<7>(file, s, e, |f| {
        let [eid, user, tsf, ingest, etype, product, price] = *f;
        if eid.0 == eid.1 {
            return;
        }
        let field = |r: (usize, usize)| &file[r.0..r.1];
        let ingest = ts::parse(field(ingest));
        let (eid_lo, eid_hi) = split_key(scan::pack_at(file, eid.0, eid.1));
        let mut rec = ERec {
            eid_lo,
            eid_hi,
            user_lo: 0,
            user_hi: 0,
            meta: (ingest_code(ingest) << (CAT_BITS + 1)) | (CAT_NONE << 1),
            ts: 0,
            price: 0,
        };
        'validate: {
            let Some(ingest) = ingest else { break 'validate };
            if user.0 == user.1 || product.0 == product.1 {
                break 'validate;
            }
            let Some(t) = ts::parse(field(tsf)) else { break 'validate };
            if t < ctx.ws || t >= ctx.we || ingest - t > LATE {
                break 'validate;
            }
            let purchase = match field(etype) {
                b"view" | b"cart" => false,
                b"purchase" => true,
                _ => break 'validate,
            };
            if purchase {
                let Some(p) = parse_price(field(price)) else { break 'validate };
                rec.price = p;
                let cat = *ctx.prod_map.get(&scan::pack_at(file, product.0, product.1)).unwrap_or(&ctx.unknown_cat);
                rec.meta = (rec.meta & !(CAT_NONE << 1)) | ((cat as u64) << 1);
            }
            rec.ts = (t - ctx.base) as u32;
            (rec.user_lo, rec.user_hi) = split_key(scan::pack_at(file, user.0, user.1));
            rec.meta |= 1;
        }
        let p = ctx.ppart(rec.eid());
        acc.ev.push(p, rec);
    });
}

#[inline(never)]
fn parse_clicks(file: &[u8], s: usize, e: usize, ctx: &Ctx, acc: &mut A1) {
    scan::scan::<4>(file, s, e, |f| {
        let [cid, user, camp, tsf] = *f;
        if cid.0 == cid.1 || user.0 == user.1 {
            return;
        }
        let Some(t) = ts::parse(&file[tsf.0..tsf.1]) else { return };
        if t < ctx.base || t >= ctx.we {
            return;
        }
        let Some(&camp) = ctx.camp_map.get(&scan::pack_at(file, camp.0, camp.1)) else {
            return;
        };
        let (u_lo, u_hi) = split_key(scan::pack_at(file, user.0, user.1));
        let (c_lo, c_hi) = split_key(scan::pack_at(file, cid.0, cid.1));
        acc.ck.push(CRaw { c_lo, c_hi, u_lo, u_hi, ts: (t - ctx.base) as u32, camp });
    });
}

// ---------------------------------------------------------------------------
// Dedup pass

struct A2 {
    ev: Vec<Vec<PRec>>,
    ck: Vec<Vec<CRec>>,
    table: Vec<u32>,
    lost: Vec<u64>,
}

#[inline(never)]
fn dedup_partition(p: usize, a1s: &[A1], ctx: &Ctx, links: &ShardedMap, out: &mut A2) {
    let n: usize = a1s.iter().map(|a| a.ev.outs[p].len()).sum();
    if n == 0 {
        return;
    }
    // Keep, per event_id, the copy with the smallest ingest_ts. A slot holds a
    // tag (hash bits) and the number of a record, counted from 1 across the
    // workers' lists, in 32 bits, so the table stays in L2; records are only
    // read when the tags match. A copy that loses is marked in `lost`.
    assert!(n < (1 << 31), "partition too large");
    let cap = (n * 2).next_power_of_two();
    let mask = cap - 1;
    let ib = usize::BITS - n.leading_zeros();
    let imask = (1u32 << ib) - 1;
    let table = &mut out.table;
    table.clear();
    table.resize(cap, 0);
    let lost = &mut out.lost;
    lost.clear();
    lost.resize(n.div_ceil(64), 0);
    let rec_at = |mut i: usize| -> &ERec {
        for a in a1s {
            let v = &a.ev.outs[p];
            if i < v.len() {
                return &v[i];
            }
            i -= v.len();
        }
        unreachable!()
    };
    let mut idx = 0u32;
    for a in a1s {
        for r in &a.ev.outs[p] {
            idx += 1;
            let k = r.eid();
            let hk = keys::hash(k);
            // The partition is the top bits of the hash and the slot the low
            // ones: take the tag from the middle.
            let tag = ((hk >> 20) as u32) & !imask;
            let mut h = hk as usize & mask;
            loop {
                let t = table[h];
                if t == 0 {
                    table[h] = tag | idx;
                    break;
                }
                if t & !imask == tag {
                    let j = (t & imask) as usize - 1;
                    let tr = rec_at(j);
                    if tr.eid() == k {
                        let loser = if r.ingest_code() < tr.ingest_code() {
                            table[h] = tag | idx;
                            j
                        } else {
                            idx as usize - 1
                        };
                        lost[loser / 64] |= 1 << (loser % 64);
                        break;
                    }
                }
                h = (h + 1) & mask;
            }
        }
    }
    // Survivors in record order: kept copies that are valid and not late.
    let mut batch: [(Key, u32, u32, u16); BATCH] = [(0, 0, 0, 0); BATCH];
    let mut nb = 0;
    let flush = |batch: &[(Key, u32, u32, u16)], out: &mut A2| {
        for b in batch {
            links.prefetch(b.0);
        }
        for &(user, ts, price, cat) in batch {
            let person = links.get_or_self(user);
            let (q, key) = ctx.qpart_key(person, ts);
            let (p_lo, p_hi) = split_key(person);
            out.ev[q].push(PRec { key, p_lo, p_hi, price, cat });
        }
    };
    let mut i = 0usize;
    for a in a1s {
        for r in &a.ev.outs[p] {
            let lost = out.lost[i / 64] >> (i % 64) & 1 != 0;
            i += 1;
            if !r.ok() || lost {
                continue;
            }
            batch[nb] = (join_key(r.user_lo, r.user_hi), r.ts, r.price, r.cat());
            nb += 1;
            if nb == BATCH {
                flush(&batch, out);
                nb = 0;
            }
        }
    }
    flush(&batch[..nb], out);
}

#[inline(never)]
fn map_clicks(recs: &[CRaw], ctx: &Ctx, links: &ShardedMap, out: &mut A2) {
    for chunk in recs.chunks(BATCH) {
        for r in chunk {
            links.prefetch(join_key(r.u_lo, r.u_hi));
        }
        for r in chunk {
            let person = links.get_or_self(join_key(r.u_lo, r.u_hi));
            let (q, key) = ctx.qpart_key(person, r.ts);
            let (p_lo, p_hi) = split_key(person);
            out.ck[q].push(CRec { key, p_lo, p_hi, c_lo: r.c_lo, c_hi: r.c_hi, camp: r.camp });
        }
    }
}

// ---------------------------------------------------------------------------
// Sessions and attribution

/// Per-worker state of the person pass.
struct A3 {
    rows: Vec<UserRow>,
    /// [campaign (organic last)][day] -> sessions, purchasing, revenue, assisted.
    camp: Vec<[u64; 4]>,
    counts: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    cats: Vec<(u16, u64)>,
    ev: Vec<PRec>,
    ck: Vec<CRec>,
    counts_sort: Vec<u32>,
}

impl A3 {
    fn new(ctx: &Ctx) -> A3 {
        A3 {
            rows: Vec::new(),
            camp: vec![[0; 4]; (ctx.ncamp() + 1) * ctx.ndays],
            counts: vec![0; ctx.ncamp()],
            pos: vec![0; ctx.ncamp()],
            active: Vec::new(),
            cats: Vec::new(),
            ev: Vec::new(),
            ck: Vec::new(),
            counts_sort: Vec::new(),
        }
    }

    #[inline(always)]
    fn add(&mut self, c: u32) {
        let cu = c as usize;
        self.counts[cu] += 1;
        if self.counts[cu] == 1 {
            self.pos[cu] = self.active.len() as u32;
            self.active.push(c);
        }
    }

    #[inline(always)]
    fn remove(&mut self, c: u32) {
        let cu = c as usize;
        self.counts[cu] -= 1;
        if self.counts[cu] == 0 {
            let p = self.pos[cu] as usize;
            let last = self.active.pop().unwrap();
            if last != c {
                self.active[p] = last;
                self.pos[last as usize] = p as u32;
            }
        }
    }

    fn flush_day(&mut self, person: Key, day: i64, sessions: u32, events: u32, revenue: u64) {
        let mut top = NO_CAT;
        let mut best = 0u64;
        for &(c, s) in &self.cats {
            if s > best || (s == best && c < top) {
                best = s;
                top = c;
            }
        }
        self.cats.clear();
        self.rows.push(UserRow { person, day: day as i32, sessions, events, revenue, top });
    }
}

#[inline(never)]
fn process_person(person: Key, evs: &[PRec], clicks: &[CRec], ctx: &Ctx, acc: &mut A3) {
    let base_day = ctx.base.div_euclid(DAY_MS);
    let base_rem = ctx.base.rem_euclid(DAY_MS);
    let ndays = ctx.ndays;
    let organic = ctx.ncamp();
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let (mut ud_sessions, mut ud_events, mut ud_revenue) = (0u32, 0u32, 0u64);
    let mut k = 0;
    while k < evs.len() {
        let start = ctx.ts_of(evs[k].key);
        while hi < clicks.len() && ctx.ts_of(clicks[hi].key) <= start {
            acc.add(clicks[hi].camp);
            hi += 1;
        }
        let lower = start - REACH;
        while lo < hi && ctx.ts_of(clicks[lo].key) < lower {
            acc.remove(clicks[lo].camp);
            lo += 1;
        }
        let credit = if lo < hi { clicks[hi - 1].camp as usize } else { organic };

        let day = base_day + (base_rem + start as i64).div_euclid(DAY_MS);
        if day != cur_day {
            if cur_day != i64::MIN {
                acc.flush_day(person, cur_day, ud_sessions, ud_events, ud_revenue);
            }
            cur_day = day;
            ud_sessions = 0;
            ud_events = 0;
            ud_revenue = 0;
        }
        let day_idx = (day - ctx.day0) as usize;
        debug_assert!(day_idx < ndays);

        let mut last = start;
        let mut n = 0u32;
        let mut revenue = 0u64;
        let mut purchased = false;
        while k < evs.len() {
            let e = &evs[k];
            let t = ctx.ts_of(e.key);
            if t - last > GAP {
                break;
            }
            n += 1;
            if e.price > 0 {
                revenue += e.price as u64;
                purchased = true;
                match acc.cats.iter_mut().find(|x| x.0 == e.cat) {
                    Some(x) => x.1 += e.price as u64,
                    None => acc.cats.push((e.cat, e.price as u64)),
                }
            }
            last = t;
            k += 1;
        }
        ud_sessions += 1;
        ud_events += n;
        ud_revenue += revenue;
        let cell = &mut acc.camp[credit * ndays + day_idx];
        cell[0] += 1;
        cell[1] += purchased as u64;
        cell[2] += revenue;
        for i in 0..acc.active.len() {
            let c = acc.active[i] as usize;
            acc.camp[c * ndays + day_idx][3] += 1;
        }
    }
    if cur_day != i64::MIN {
        acc.flush_day(person, cur_day, ud_sessions, ud_events, ud_revenue);
    }
    for i in 0..acc.active.len() {
        let c = acc.active[i] as usize;
        acc.counts[c] = 0;
    }
    acc.active.clear();
}

#[inline(never)]
/// Concatenates `parts` into `out`, sorted by `cmp`, which must order by
/// `key` first. A counting sort on the key's top bits leaves small buckets
/// (usually one person each) that are finished by insertion sort.
fn bucket_sort<'a, T: Copy + 'a>(
    parts: impl Iterator<Item = &'a [T]> + Clone,
    out: &mut Vec<T>,
    counts: &mut Vec<u32>,
    key: impl Fn(&T) -> u64,
    cmp: impl Fn(&T, &T) -> std::cmp::Ordering,
) {
    let n: usize = parts.clone().map(|p| p.len()).sum();
    out.clear();
    if n == 0 {
        return;
    }
    let bits = (usize::BITS - (n / 4).max(2).leading_zeros()).clamp(1, 16);
    let shift = 64 - bits;
    let nb = 1usize << bits;
    counts.clear();
    counts.resize(nb + 1, 0);
    for p in parts.clone() {
        for r in p {
            counts[(key(r) >> shift) as usize + 1] += 1;
        }
    }
    for i in 0..nb {
        counts[i + 1] += counts[i];
    }
    out.reserve(n);
    let dst = out.as_mut_ptr();
    {
        let mut pos: Vec<u32> = counts[..nb].to_vec();
        for p in parts {
            for r in p {
                let b = (key(r) >> shift) as usize;
                unsafe { dst.add(pos[b] as usize).write(*r) };
                pos[b] += 1;
            }
        }
    }
    unsafe { out.set_len(n) };
    for b in 0..nb {
        let (s, e) = (counts[b] as usize, counts[b + 1] as usize);
        let bucket = &mut out[s..e];
        if bucket.len() <= 24 {
            for i in 1..bucket.len() {
                let x = bucket[i];
                let mut j = i;
                while j > 0 && cmp(&bucket[j - 1], &x) == std::cmp::Ordering::Greater {
                    bucket[j] = bucket[j - 1];
                    j -= 1;
                }
                bucket[j] = x;
            }
        } else {
            bucket.sort_unstable_by(&cmp);
        }
    }
}

#[inline(never)]
fn process_partition(q: usize, a2s: &[A2], ctx: &Ctx, acc: &mut A3) {
    let mut ev = std::mem::take(&mut acc.ev);
    let mut ck = std::mem::take(&mut acc.ck);
    // Gather and sort in one go: by key (person hash, then ts), clicks then by
    // click_id descending.
    bucket_sort(a2s.iter().map(|a| &a.ev[q][..]), &mut ev, &mut acc.counts_sort, |r| r.key, |a, b| {
        a.key.cmp(&b.key)
    });
    bucket_sort(a2s.iter().map(|a| &a.ck[q][..]), &mut ck, &mut acc.counts_sort, |r| r.key, |a, b| {
        a.key.cmp(&b.key).then_with(|| keys::cmp(b.cid(), a.cid()))
    });
    if !ev.is_empty() {
        let tb = ctx.ts_bits;
        // Two people whose hashes agree in the key bits would interleave: detect
        // that and fall back to sorting by the full person ID.
        let collide_ev = ev.windows(2).any(|w| (w[0].key ^ w[1].key) >> tb == 0 && w[0].person() != w[1].person());
        let collide_ck = ck.windows(2).any(|w| (w[0].key ^ w[1].key) >> tb == 0 && w[0].person() != w[1].person());
        if collide_ev || collide_ck {
            ev.sort_unstable_by_key(|r| (r.key >> tb, r.p_hi, r.p_lo, r.key));
            ck.sort_unstable_by(|a, b| {
                (a.key >> tb, a.p_hi, a.p_lo, a.key)
                    .cmp(&(b.key >> tb, b.p_hi, b.p_lo, b.key))
                    .then_with(|| keys::cmp(b.cid(), a.cid()))
            });
        }
        // People in (hash bits, id) order on both sides.
        let ord_e = |r: &PRec| (r.key >> tb, r.p_hi, r.p_lo);
        let ord_c = |r: &CRec| (r.key >> tb, r.p_hi, r.p_lo);
        let mut ci = 0;
        let mut i = 0;
        while i < ev.len() {
            let p = ev[i].person();
            let pe = ord_e(&ev[i]);
            let mut j = i + 1;
            while j < ev.len() && ev[j].person() == p {
                j += 1;
            }
            while ci < ck.len() && ord_c(&ck[ci]) < pe {
                ci += 1;
            }
            let cs = ci;
            while ci < ck.len() && ck[ci].person() == p {
                ci += 1;
            }
            process_person(p, &ev[i..j], &ck[cs..ci], ctx, acc);
            i = j;
        }
    }
    acc.ev = ev;
    acc.ck = ck;
}

// ---------------------------------------------------------------------------
// Output

fn write_user_daily(dir: &Path, part: usize, rows: &[UserRow], ctx: &Ctx) {
    use out::{Column, Kind};
    let n = rows.len();
    let mut ids = Column::new("user_id", Kind::Str, n);
    let mut days = Column::new("day", Kind::Date, n);
    let mut sessions = Column::new("sessions", Kind::I32, n);
    let mut events = Column::new("events", Kind::I32, n);
    let mut revenue = Column::new("revenue_cents", Kind::I64, n);
    let mut tops = Column::new("top_category", Kind::OptStr, n);
    let mut name = Vec::with_capacity(32);
    for (i, r) in rows.iter().enumerate() {
        name.clear();
        keys::write_bytes(r.person, &mut name);
        ids.str(&name);
        days.i32(r.day);
        sessions.i32(r.sessions as i32);
        events.i32(r.events as i32);
        revenue.i64(r.revenue as i64);
        tops.opt_str(i, (r.top != NO_CAT).then(|| ctx.cats[r.top as usize].as_slice()));
    }
    out::write(&dir.join(format!("part-{part:05}.parquet")), n, &[ids, days, sessions, events, revenue, tops]);
}

fn write_campaign_daily(dir: &Path, camp: &[[u64; 4]], ctx: &Ctx) {
    use out::{Column, Kind};
    let cells: Vec<(usize, usize)> = (0..=ctx.ncamp())
        .flat_map(|c| (0..ctx.ndays).map(move |d| (c, d)))
        .filter(|&(c, d)| {
            let cell = camp[c * ctx.ndays + d];
            cell[0] != 0 || cell[3] != 0
        })
        .collect();
    let n = cells.len();
    let mut ids = Column::new("campaign_id", Kind::Str, n);
    let mut channels = Column::new("channel", Kind::Str, n);
    let mut days = Column::new("day", Kind::Date, n);
    let mut cols = [
        Column::new("sessions", Kind::I64, n),
        Column::new("purchasing_sessions", Kind::I64, n),
        Column::new("revenue_cents", Kind::I64, n),
        Column::new("assisted_sessions", Kind::I64, n),
    ];
    for &(c, d) in &cells {
        if c == ctx.ncamp() {
            ids.str(b"organic");
            channels.str(b"organic");
        } else {
            ids.str(&ctx.camp_ids[c]);
            channels.str(&ctx.camp_channels[c]);
        }
        days.i32((ctx.day0 + d as i64) as i32);
        for (col, v) in cols.iter_mut().zip(camp[c * ctx.ndays + d]) {
            col.i64(v as i64);
        }
    }
    let [s, p, r, a] = cols;
    out::write(&dir.join("part-00000.parquet"), n, &[ids, channels, days, s, p, r, a]);
}

// ---------------------------------------------------------------------------

fn main() {
    let t0 = Instant::now();
    let cfg = Cfg::load();
    rayon::ThreadPoolBuilder::new().num_threads(cfg.threads).build_global().unwrap();
    let nw = cfg.threads;

    let (ws, we) = read_manifest(&cfg.data.join("manifest.json"));
    let base = ws - 7 * DAY_MS;
    assert!(we > ws && we - base < u32::MAX as i64, "window too long for u32 offsets");
    let ts_bits = 64 - ((we - base) as u64).leading_zeros();
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - day0 + 1) as usize;

    let (camp_map, camp_ids, camp_channels) = load_campaigns(&cfg.data.join("campaigns.csv"));
    let (prod_map, cats, unknown_cat) = load_products(&cfg.data.join("products.csv"));
    let ev_files = map_dir(&cfg.data.join("events"));
    let ck_files = map_dir(&cfg.data.join("clicks"));
    let lk_files = map_dir(&cfg.data.join("links"));
    let ctx = Ctx {
        ws,
        we,
        base,
        day0,
        ndays,
        ts_bits,
        camp_map,
        camp_ids,
        camp_channels,
        prod_map,
        cats,
        unknown_cat,
        pbits: cfg.pbits,
        qbits: cfg.qbits,
    };
    let np = 1usize << cfg.pbits;
    let nq = 1usize << cfg.qbits;
    log!(t0, "lookups: {} campaigns, {} products, {} categories", ctx.camp_ids.len(), ctx.prod_map.len(), ctx.cats.len());

    // Phase 1: links, in parallel with parsing events and clicks.
    let mut jobs: Vec<(Kind, usize, usize, usize)> = Vec::new();
    for (fi, m) in ev_files.iter().enumerate() {
        jobs.extend(csv::chunks(m, cfg.chunk).into_iter().map(|(s, e)| (Kind::Events, fi, s, e)));
    }
    for (fi, m) in ck_files.iter().enumerate() {
        jobs.extend(csv::chunks(m, cfg.chunk).into_iter().map(|(s, e)| (Kind::Clicks, fi, s, e)));
    }
    jobs.sort_by_key(|j| std::cmp::Reverse(j.3 - j.2));
    let ev_bytes: usize = ev_files.iter().map(|m| m.len()).sum();
    let ck_bytes: usize = ck_files.iter().map(|m| m.len()).sum();
    let ev_reserve = ev_bytes / 64 / nw / np * 5 / 4 + 16;
    let ck_reserve = ck_bytes / 40 / nw * 5 / 4 + 16;
    // Links get their own small pool so they overlap the parse instead of
    // waiting for a free worker.
    let link_pool = rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap();
    let release = std::env::var_os("SS_KEEP").is_none();
    let (links, a1s) = std::thread::scope(|scope| {
        let handle = scope.spawn(|| {
            let l = link_pool.install(|| build_links(&lk_files, cfg.chunk));
            log!(t0, "links: {} table slots", l.slots());
            l
        });
        let a1s = workers_then(
            env_usize("SS_PARSE_THREADS").unwrap_or(nw),
            jobs.len(),
            || A1 { ev: scatter::Scatter::new(np, ev_reserve), ck: Vec::with_capacity(ck_reserve) },
            |acc, j| {
                let (kind, fi, s, e) = jobs[j];
                let file = match kind {
                    Kind::Events => &ev_files[fi],
                    Kind::Clicks => &ck_files[fi],
                };
                match kind {
                    Kind::Events => parse_events(file, s, e, &ctx, acc),
                    Kind::Clicks => parse_clicks(file, s, e, &ctx, acc),
                }
                // Unmap the pages that lie wholly inside this piece now, on
                // this worker, rather than all of them at exit on one thread.
                if release {
                    let (a, b) = (s.next_multiple_of(4096), e / 4096 * 4096);
                    if a < b {
                        let _ = unsafe { file.unchecked_advise_range(memmap2::UncheckedAdvice::DontNeed, a, b - a) };
                    }
                }
            },
            |acc, _| acc.ev.finish(),
        );
        log!(t0, "parsed");
        (handle.join().unwrap(), a1s)
    });
    if std::env::var_os("SS_DEBUG").is_some() {
        let aligned = a1s.iter().flat_map(|a| a.ev.outs.iter()).filter(|v| v.as_ptr() as usize % 64 == 0).count();
        log!(t0, "scatter buffers 64-byte aligned: {aligned} of {}", a1s.len() * np);
    }
    let n_ev: usize = a1s.iter().map(|a| a.ev.outs.iter().map(|v| v.len()).sum::<usize>()).sum();
    let n_ck: usize = a1s.iter().map(|a| a.ck.len()).sum();
    log!(t0, "phase 1: {} event lines, {} valid clicks", n_ev, n_ck);

    // Phase 2: dedup events per event_id partition; map clicks to people.
    let ck_jobs: Vec<(usize, usize, usize)> = a1s
        .iter()
        .enumerate()
        .flat_map(|(w, a)| (0..a.ck.len()).step_by(1 << 16).map(move |s| (w, s, (s + (1 << 16)).min(a.ck.len()))))
        .collect();
    let ev_reserve2 = n_ev / nw / nq * 5 / 4 + 16;
    let ck_reserve2 = n_ck / nw / nq * 5 / 4 + 16;
    let a2s = workers_then(
        nw,
        np + ck_jobs.len(),
        || A2 {
            ev: (0..nq).map(|_| Vec::with_capacity(ev_reserve2)).collect(),
            ck: (0..nq).map(|_| Vec::with_capacity(ck_reserve2)).collect(),
            table: Vec::new(),
            lost: Vec::new(),
        },
        |acc, j| {
            if j < np {
                dedup_partition(j, &a1s, &ctx, &links, acc);
            } else {
                let (w, s, e) = ck_jobs[j - np];
                map_clicks(&a1s[w].ck[s..e], &ctx, &links, acc);
            }
        },
        |_, _| {},
    );
    let n_surv: usize = a2s.iter().map(|a| a.ev.iter().map(|v| v.len()).sum::<usize>()).sum();
    log!(t0, "phase 2: {} surviving events", n_surv);

    // Phase 3: sessions and attribution per person partition. Each worker
    // writes its own user_daily file as soon as it runs out of partitions
    // (worker 0 always writes one, so the table has a file even when empty).
    let ud_dir = cfg.out.join("user_daily");
    let cd_dir = cfg.out.join("campaign_daily");
    fs::create_dir_all(&ud_dir).expect("create user_daily");
    fs::create_dir_all(&cd_dir).expect("create campaign_daily");
    let a3s = workers_then(
        nw,
        nq,
        || A3::new(&ctx),
        |acc, q| {
            let t = Instant::now();
            let n: usize = a2s.iter().map(|a| a.ev[q].len()).sum();
            process_partition(q, &a2s, &ctx, acc);
            if std::env::var_os("SS_DEBUG").is_some() && t.elapsed().as_millis() > 15 {
                log!(t0, "phase 3 partition {q}: {n} events, {} ms", t.elapsed().as_millis());
            }
        },
        |acc, w| {
            if std::env::var_os("SS_DEBUG").is_some() {
                log!(t0, "phase 3 worker {w} done");
            }
            if w == 0 || !acc.rows.is_empty() {
                write_user_daily(&ud_dir, w, &acc.rows, &ctx);
            }
            if std::env::var_os("SS_DEBUG").is_some() {
                log!(t0, "phase 3 worker {w} wrote {} rows", acc.rows.len());
            }
        },
    );
    log!(t0, "phase 3: {} user-day rows", a3s.iter().map(|a| a.rows.len()).sum::<usize>());
    let mut camp = vec![[0u64; 4]; (ctx.ncamp() + 1) * ctx.ndays];
    for a in &a3s {
        for (x, y) in camp.iter_mut().zip(&a.camp) {
            for i in 0..4 {
                x[i] += y[i];
            }
        }
    }
    write_campaign_daily(&cd_dir, &camp, &ctx);
    if std::env::var_os("SS_CHECK").is_some() {
        let us: u64 = a3s.iter().flat_map(|a| a.rows.iter()).map(|r| r.sessions as u64).sum();
        let ur: u64 = a3s.iter().flat_map(|a| a.rows.iter()).map(|r| r.revenue).sum();
        let cs: u64 = camp.iter().map(|c| c[0]).sum();
        let cr: u64 = camp.iter().map(|c| c[2]).sum();
        let ca: u64 = camp.iter().map(|c| c[3]).sum();
        let credited: u64 = camp[..ctx.ncamp() * ctx.ndays].iter().map(|c| c[0]).sum();
        log!(t0, "check: sessions {us}={cs} revenue {ur}={cr} assisted {ca}>={credited}");
        assert!(us == cs && ur == cr && ca >= credited);
    }
    log!(t0, "done");
    std::process::exit(0);
}
