//! Shopping Sessions pipeline. See rules.md for the spec and README.md for the design.

mod csv;
mod keys;
mod out;
mod ts;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use memmap2::Mmap;
use parquet::data_type::ByteArray;
use rayon::prelude::*;

use keys::{Key, KeyMap};
use ts::DAY_MS;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const GAP: u32 = 1_800_000;
const REACH: u32 = 604_800_000;
const LATE: i64 = 86_400_000;
const PRICE_MAX: u64 = 10_000_000;
const NO_CAT: u16 = u16::MAX;

// ---------------------------------------------------------------------------
// Records

#[inline]
fn split_key(k: Key) -> (u64, u64) {
    (k as u64, (k >> 64) as u64)
}

#[inline]
fn join_key(lo: u64, hi: u64) -> Key {
    (lo as u128) | ((hi as u128) << 64)
}

/// A well-formed event line with a non-empty event_id, before dedup.
#[derive(Clone, Copy)]
struct ERec {
    eid_lo: u64,
    eid_hi: u64,
    user_lo: u64,
    user_hi: u64,
    ingest: i64,
    ts: u32,
    price: u32,
    cat: u16,
    ok: bool,
}

impl ERec {
    #[inline]
    fn eid(&self) -> Key {
        join_key(self.eid_lo, self.eid_hi)
    }
}

/// A surviving event, keyed by person.
#[derive(Clone, Copy)]
struct PRec {
    p_lo: u64,
    p_hi: u64,
    ts: u32,
    price: u32,
    cat: u16,
}

impl PRec {
    #[inline]
    fn person(&self) -> Key {
        join_key(self.p_lo, self.p_hi)
    }
}

/// A valid click, keyed by person.
#[derive(Clone, Copy)]
struct CRec {
    p_lo: u64,
    p_hi: u64,
    c_lo: u64,
    c_hi: u64,
    ts: u32,
    camp: u32,
}

impl CRec {
    #[inline]
    fn person(&self) -> Key {
        join_key(self.p_lo, self.p_hi)
    }
    #[inline]
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
            threads: env_usize("SS_THREADS").unwrap_or(threads),
            chunk: env_usize("SS_CHUNK").unwrap_or(8 << 20),
            pbits: env_usize("SS_PBITS").unwrap_or(9) as u32,
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
    camp_map: KeyMap<u32>,
    camp_ids: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
    prod_map: KeyMap<u16>,
    cats: Vec<Vec<u8>>,
    unknown_cat: u16,
    link_map: KeyMap<Key>,
    pbits: u32,
    qbits: u32,
}

impl Ctx {
    #[inline]
    fn ncamp(&self) -> usize {
        self.camp_ids.len()
    }
    #[inline]
    fn person(&self, user: Key) -> Key {
        match self.link_map.get(&user) {
            Some(&p) => p,
            None => user,
        }
    }
    #[inline]
    fn ppart(&self, k: Key) -> usize {
        if self.pbits == 0 { 0 } else { (keys::hash(k) >> (64 - self.pbits)) as usize }
    }
    #[inline]
    fn qpart(&self, k: Key) -> usize {
        // Different bits from ppart, so a dedup partition spreads over all person partitions.
        if self.qbits == 0 { 0 } else { ((keys::hash(k) >> 7) & ((1u64 << self.qbits) - 1)) as usize }
    }
}

macro_rules! log {
    ($t0:expr, $($arg:tt)*) => {
        eprintln!("[{:>8.3}s] {}", $t0.elapsed().as_secs_f64(), format!($($arg)*));
    };
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
    unsafe { Mmap::map(&f).ok() }
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
    assert!(cats.len() < NO_CAT as usize, "too many categories");
    let idx = |c: &[u8]| cats.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u16;
    let mut map = KeyMap::default();
    for (k, c) in &rows {
        map.entry(*k).or_insert_with(|| idx(c));
    }
    let unknown = idx(b"unknown");
    (map, cats, unknown)
}

/// Union-find over links/: maps every linked ID whose person name differs from
/// itself to that name (the byte-wise smallest ID of the person).
fn build_links(files: &[Mmap], chunk: usize) -> KeyMap<Key> {
    let jobs: Vec<(usize, usize, usize)> = files
        .iter()
        .enumerate()
        .flat_map(|(fi, m)| csv::chunks(m, chunk).into_iter().map(move |(s, e)| (fi, s, e)))
        .collect();
    let pairs: Vec<Vec<(Key, Key)>> = jobs
        .par_iter()
        .map(|&(fi, s, e)| {
            let mut v = Vec::new();
            csv::for_lines(&files[fi][s..e], |line| {
                if let Some([a, b]) = csv::split::<2>(line) {
                    if !a.is_empty() && !b.is_empty() && a != b {
                        v.push((keys::pack(a), keys::pack(b)));
                    }
                }
            });
            v
        })
        .collect();

    let mut index: KeyMap<u32> = KeyMap::default();
    let mut ids: Vec<Key> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut size: Vec<u32> = Vec::new();
    let mut intern = |k: Key, ids: &mut Vec<Key>, parent: &mut Vec<u32>, size: &mut Vec<u32>| -> u32 {
        *index.entry(k).or_insert_with(|| {
            let i = ids.len() as u32;
            ids.push(k);
            parent.push(i);
            size.push(1);
            i
        })
    };
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
    for v in &pairs {
        for &(a, b) in v {
            let ia = intern(a, &mut ids, &mut parent, &mut size);
            let ib = intern(b, &mut ids, &mut parent, &mut size);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                let (big, small) = if size[ra as usize] >= size[rb as usize] { (ra, rb) } else { (rb, ra) };
                parent[small as usize] = big;
                size[big as usize] += size[small as usize];
            }
        }
    }
    let n = ids.len();
    let mut name: Vec<Key> = vec![0; n];
    let mut has = vec![false; n];
    let mut roots = vec![0u32; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        roots[i] = r as u32;
        if !has[r] || keys::cmp(ids[i], name[r]) == std::cmp::Ordering::Less {
            name[r] = ids[i];
            has[r] = true;
        }
    }
    let mut map = KeyMap::default();
    map.reserve(n);
    for i in 0..n {
        let nm = name[roots[i] as usize];
        if nm != ids[i] {
            map.insert(ids[i], nm);
        }
    }
    map
}

// ---------------------------------------------------------------------------
// Parse pass

#[inline]
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

struct Acc1 {
    ev: Vec<Vec<ERec>>,
    ck: Vec<Vec<CRec>>,
}

fn parse_events(buf: &[u8], ctx: &Ctx, acc: &mut Acc1) {
    csv::for_lines(buf, |line| {
        let Some([eid, user, tsf, ingest, etype, product, price]) = csv::split::<7>(line) else {
            return;
        };
        if eid.is_empty() {
            return;
        }
        let ingest = ts::parse(ingest);
        let mut rec = ERec {
            eid_lo: 0,
            eid_hi: 0,
            user_lo: 0,
            user_hi: 0,
            ingest: ingest.unwrap_or(i64::MAX),
            ts: 0,
            price: 0,
            cat: NO_CAT,
            ok: false,
        };
        (rec.eid_lo, rec.eid_hi) = split_key(keys::pack(eid));
        'validate: {
            let Some(ingest) = ingest else { break 'validate };
            if user.is_empty() || product.is_empty() {
                break 'validate;
            }
            let Some(t) = ts::parse(tsf) else { break 'validate };
            if t < ctx.ws || t >= ctx.we {
                break 'validate;
            }
            let purchase = match etype {
                b"view" | b"cart" => false,
                b"purchase" => true,
                _ => break 'validate,
            };
            if purchase {
                let Some(p) = parse_price(price) else { break 'validate };
                rec.price = p;
                rec.cat = *ctx.prod_map.get(&keys::pack(product)).unwrap_or(&ctx.unknown_cat);
            }
            if ingest - t > LATE {
                break 'validate;
            }
            rec.ts = (t - ctx.base) as u32;
            (rec.user_lo, rec.user_hi) = split_key(keys::pack(user));
            rec.ok = true;
        }
        let p = ctx.ppart(rec.eid());
        acc.ev[p].push(rec);
    });
}

fn parse_clicks(buf: &[u8], ctx: &Ctx, acc: &mut Acc1) {
    csv::for_lines(buf, |line| {
        let Some([cid, user, camp, tsf]) = csv::split::<4>(line) else {
            return;
        };
        if cid.is_empty() || user.is_empty() {
            return;
        }
        let Some(t) = ts::parse(tsf) else { return };
        if t < ctx.base || t >= ctx.we {
            return;
        }
        let Some(&camp) = ctx.camp_map.get(&keys::pack(camp)) else {
            return;
        };
        let person = ctx.person(keys::pack(user));
        let (p_lo, p_hi) = split_key(person);
        let (c_lo, c_hi) = split_key(keys::pack(cid));
        let q = ctx.qpart(person);
        acc.ck[q].push(CRec { p_lo, p_hi, c_lo, c_hi, ts: (t - ctx.base) as u32, camp });
    });
}

// ---------------------------------------------------------------------------
// Dedup pass

fn dedup_partition(p: usize, accs: &[Acc1], ctx: &Ctx, out: &mut [Vec<PRec>]) {
    let n: usize = accs.iter().map(|a| a.ev[p].len()).sum();
    if n == 0 {
        return;
    }
    let cap = (n * 2).next_power_of_two();
    let mask = cap - 1;
    let mut table: Vec<*const ERec> = vec![std::ptr::null(); cap];
    for a in accs {
        for r in &a.ev[p] {
            let k = r.eid();
            let mut h = keys::hash(k) as usize & mask;
            loop {
                let t = table[h];
                if t.is_null() {
                    table[h] = r;
                    break;
                }
                let tr = unsafe { &*t };
                if tr.eid() == k {
                    if r.ingest < tr.ingest {
                        table[h] = r;
                    }
                    break;
                }
                h = (h + 1) & mask;
            }
        }
    }
    for &t in &table {
        if t.is_null() {
            continue;
        }
        let r = unsafe { &*t };
        if !r.ok {
            continue;
        }
        let person = ctx.person(join_key(r.user_lo, r.user_hi));
        let (p_lo, p_hi) = split_key(person);
        out[ctx.qpart(person)].push(PRec { p_lo, p_hi, ts: r.ts, price: r.price, cat: r.cat });
    }
}

// ---------------------------------------------------------------------------
// Sessions and attribution

/// Per-thread state of the person pass.
struct Acc3 {
    rows: Vec<UserRow>,
    /// [campaign (organic last)][day] -> sessions, purchasing, revenue, assisted.
    camp: Vec<[u64; 4]>,
    counts: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    cats: Vec<(u16, u64)>,
}

impl Acc3 {
    fn new(ctx: &Ctx) -> Acc3 {
        Acc3 {
            rows: Vec::new(),
            camp: vec![[0; 4]; (ctx.ncamp() + 1) * ctx.ndays],
            counts: vec![0; ctx.ncamp()],
            pos: vec![0; ctx.ncamp()],
            active: Vec::new(),
            cats: Vec::new(),
        }
    }

    #[inline]
    fn add(&mut self, c: u32) {
        let cu = c as usize;
        self.counts[cu] += 1;
        if self.counts[cu] == 1 {
            self.pos[cu] = self.active.len() as u32;
            self.active.push(c);
        }
    }

    #[inline]
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

fn process_person(person: Key, evs: &[PRec], clicks: &[CRec], ctx: &Ctx, acc: &mut Acc3) {
    let base_day = ctx.base.div_euclid(DAY_MS);
    let base_rem = ctx.base.rem_euclid(DAY_MS);
    let ndays = ctx.ndays;
    let organic = ctx.ncamp();
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let (mut ud_sessions, mut ud_events, mut ud_revenue) = (0u32, 0u32, 0u64);
    let mut k = 0;
    while k < evs.len() {
        let start = evs[k].ts;
        while hi < clicks.len() && clicks[hi].ts <= start {
            acc.add(clicks[hi].camp);
            hi += 1;
        }
        let lower = start - REACH;
        while lo < hi && clicks[lo].ts < lower {
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
        while k < evs.len() && evs[k].ts - last <= GAP {
            let e = &evs[k];
            n += 1;
            if e.price > 0 {
                revenue += e.price as u64;
                purchased = true;
                match acc.cats.iter_mut().find(|x| x.0 == e.cat) {
                    Some(x) => x.1 += e.price as u64,
                    None => acc.cats.push((e.cat, e.price as u64)),
                }
            }
            last = e.ts;
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

fn process_partition(q: usize, evparts: &[Vec<Vec<PRec>>], accs1: &[Acc1], ctx: &Ctx, acc: &mut Acc3) {
    let n: usize = evparts.iter().map(|a| a[q].len()).sum();
    if n == 0 {
        return;
    }
    let mut ev: Vec<PRec> = Vec::with_capacity(n);
    for a in evparts {
        ev.extend_from_slice(&a[q]);
    }
    ev.sort_unstable_by_key(|r| (r.p_hi, r.p_lo, r.ts));
    let m: usize = accs1.iter().map(|a| a.ck[q].len()).sum();
    let mut ck: Vec<CRec> = Vec::with_capacity(m);
    for a in accs1 {
        ck.extend_from_slice(&a.ck[q]);
    }
    // By person, then ts ascending, then click_id descending, so the last
    // click at or before a session start is the latest with the smallest id.
    ck.sort_unstable_by(|a, b| {
        (a.p_hi, a.p_lo, a.ts).cmp(&(b.p_hi, b.p_lo, b.ts)).then_with(|| keys::cmp(b.cid(), a.cid()))
    });
    let mut ci = 0;
    let mut i = 0;
    while i < ev.len() {
        let p = ev[i].person();
        let mut j = i + 1;
        while j < ev.len() && ev[j].person() == p {
            j += 1;
        }
        while ci < ck.len() && ck[ci].person() < p {
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

// ---------------------------------------------------------------------------
// Output

fn write_user_daily(dir: &Path, part: usize, rows: &[UserRow], ctx: &Ctx) {
    let mut ids = Vec::with_capacity(rows.len());
    let mut days = Vec::with_capacity(rows.len());
    let mut sessions = Vec::with_capacity(rows.len());
    let mut events = Vec::with_capacity(rows.len());
    let mut revenue = Vec::with_capacity(rows.len());
    let mut tops = Vec::with_capacity(rows.len());
    let mut defs = Vec::with_capacity(rows.len());
    for r in rows {
        ids.push(ByteArray::from(keys::to_vec(r.person)));
        days.push(r.day);
        sessions.push(r.sessions as i64);
        events.push(r.events as i64);
        revenue.push(r.revenue as i64);
        if r.top == NO_CAT {
            defs.push(0);
        } else {
            defs.push(1);
            tops.push(ByteArray::from(ctx.cats[r.top as usize].clone()));
        }
    }
    out::write(
        &dir.join(format!("part-{part:05}.parquet")),
        out::USER_SCHEMA,
        vec![
            out::Col::Str(ids),
            out::Col::I32(days),
            out::Col::I64(sessions),
            out::Col::I64(events),
            out::Col::I64(revenue),
            out::Col::OptStr(tops, defs),
        ],
    );
}

fn write_campaign_daily(dir: &Path, camp: &[[u64; 4]], ctx: &Ctx) {
    let mut ids = Vec::new();
    let mut channels = Vec::new();
    let mut days = Vec::new();
    let mut cols: [Vec<i64>; 4] = Default::default();
    for c in 0..=ctx.ncamp() {
        for d in 0..ctx.ndays {
            let cell = camp[c * ctx.ndays + d];
            if cell[0] == 0 && cell[3] == 0 {
                continue;
            }
            if c == ctx.ncamp() {
                ids.push(ByteArray::from(&b"organic"[..]));
                channels.push(ByteArray::from(&b"organic"[..]));
            } else {
                ids.push(ByteArray::from(ctx.camp_ids[c].clone()));
                channels.push(ByteArray::from(ctx.camp_channels[c].clone()));
            }
            days.push((ctx.day0 + d as i64) as i32);
            for (col, v) in cols.iter_mut().zip(cell) {
                col.push(v as i64);
            }
        }
    }
    let [s, p, r, a] = cols;
    out::write(
        &dir.join("part-00000.parquet"),
        out::CAMPAIGN_SCHEMA,
        vec![
            out::Col::Str(ids),
            out::Col::Str(channels),
            out::Col::I32(days),
            out::Col::I64(s),
            out::Col::I64(p),
            out::Col::I64(r),
            out::Col::I64(a),
        ],
    );
}

// ---------------------------------------------------------------------------

fn main() {
    let t0 = Instant::now();
    let cfg = Cfg::load();
    rayon::ThreadPoolBuilder::new().num_threads(cfg.threads).build_global().unwrap();

    let (ws, we) = read_manifest(&cfg.data.join("manifest.json"));
    let base = ws - 7 * DAY_MS;
    assert!(we > ws && we - base < u32::MAX as i64, "window too long for u32 offsets");
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - day0 + 1) as usize;

    let (camp_map, camp_ids, camp_channels) = load_campaigns(&cfg.data.join("campaigns.csv"));
    let (prod_map, cats, unknown_cat) = load_products(&cfg.data.join("products.csv"));
    let ev_files = map_dir(&cfg.data.join("events"));
    let ck_files = map_dir(&cfg.data.join("clicks"));
    let lk_files = map_dir(&cfg.data.join("links"));
    log!(t0, "lookups: {} campaigns, {} products, {} categories", camp_ids.len(), prod_map.len(), cats.len());

    let link_map = build_links(&lk_files, cfg.chunk);
    log!(t0, "links: {} renamed ids", link_map.len());

    let ctx = Ctx {
        ws,
        we,
        base,
        day0,
        ndays,
        camp_map,
        camp_ids,
        camp_channels,
        prod_map,
        cats,
        unknown_cat,
        link_map,
        pbits: cfg.pbits,
        qbits: cfg.qbits,
    };
    let np = 1usize << cfg.pbits;
    let nq = 1usize << cfg.qbits;

    let mut jobs: Vec<(Kind, usize, usize, usize)> = Vec::new();
    for (fi, m) in ev_files.iter().enumerate() {
        jobs.extend(csv::chunks(m, cfg.chunk).into_iter().map(|(s, e)| (Kind::Events, fi, s, e)));
    }
    for (fi, m) in ck_files.iter().enumerate() {
        jobs.extend(csv::chunks(m, cfg.chunk).into_iter().map(|(s, e)| (Kind::Clicks, fi, s, e)));
    }
    // Biggest first, for load balance.
    jobs.sort_by_key(|j| std::cmp::Reverse(j.3 - j.2));
    let accs1: Vec<Acc1> = jobs
        .par_iter()
        .fold(
            || Acc1 { ev: (0..np).map(|_| Vec::new()).collect(), ck: (0..nq).map(|_| Vec::new()).collect() },
            |mut acc, &(kind, fi, s, e)| {
                match kind {
                    Kind::Events => parse_events(&ev_files[fi][s..e], &ctx, &mut acc),
                    Kind::Clicks => parse_clicks(&ck_files[fi][s..e], &ctx, &mut acc),
                }
                acc
            },
        )
        .collect();
    let n_ev: usize = accs1.iter().map(|a| a.ev.iter().map(|v| v.len()).sum::<usize>()).sum();
    let n_ck: usize = accs1.iter().map(|a| a.ck.iter().map(|v| v.len()).sum::<usize>()).sum();
    log!(t0, "parsed: {} event lines, {} valid clicks, {} accumulators", n_ev, n_ck, accs1.len());

    let evparts: Vec<Vec<Vec<PRec>>> = (0..np)
        .into_par_iter()
        .fold(
            || (0..nq).map(|_| Vec::new()).collect::<Vec<Vec<PRec>>>(),
            |mut out, p| {
                dedup_partition(p, &accs1, &ctx, &mut out);
                out
            },
        )
        .collect();
    let n_surv: usize = evparts.iter().map(|a| a.iter().map(|v| v.len()).sum::<usize>()).sum();
    log!(t0, "dedup: {} surviving events", n_surv);

    let accs3: Vec<Acc3> = (0..nq)
        .into_par_iter()
        .fold(
            || Acc3::new(&ctx),
            |mut acc, q| {
                process_partition(q, &evparts, &accs1, &ctx, &mut acc);
                acc
            },
        )
        .collect();
    log!(t0, "sessions: {} user-day rows", accs3.iter().map(|a| a.rows.len()).sum::<usize>());

    let ud_dir = cfg.out.join("user_daily");
    let cd_dir = cfg.out.join("campaign_daily");
    fs::create_dir_all(&ud_dir).expect("create user_daily");
    fs::create_dir_all(&cd_dir).expect("create campaign_daily");
    let mut camp = vec![[0u64; 4]; (ctx.ncamp() + 1) * ctx.ndays];
    for a in &accs3 {
        for (x, y) in camp.iter_mut().zip(&a.camp) {
            for i in 0..4 {
                x[i] += y[i];
            }
        }
    }
    let non_empty: Vec<&Acc3> = accs3.iter().filter(|a| !a.rows.is_empty()).collect();
    rayon::join(
        || {
            if non_empty.is_empty() {
                write_user_daily(&ud_dir, 0, &[], &ctx);
            } else {
                non_empty.par_iter().enumerate().for_each(|(i, a)| write_user_daily(&ud_dir, i, &a.rows, &ctx));
            }
        },
        || write_campaign_daily(&cd_dir, &camp, &ctx),
    );
    if std::env::var_os("SS_CHECK").is_some() {
        let us: u64 = accs3.iter().flat_map(|a| a.rows.iter()).map(|r| r.sessions as u64).sum();
        let ur: u64 = accs3.iter().flat_map(|a| a.rows.iter()).map(|r| r.revenue).sum();
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
