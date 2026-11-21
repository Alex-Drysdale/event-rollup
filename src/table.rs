//! Open-addressing maps keyed by `Key`, split into shards by the top bits of
//! the hash so that each shard can be built by its own thread. Lookups can be
//! batched: `prefetch` every key of a batch first, then `get` them, so the
//! cache misses overlap. Key 0 (the empty string) marks an empty slot, which is
//! fine because empty IDs are never stored.

use rayon::prelude::*;

use crate::keys::{self, Key};

pub const SHARD_BITS: u32 = 6;
pub const SHARDS: usize = 1 << SHARD_BITS;

#[inline(always)]
pub fn shard_of(h: u64) -> usize {
    (h >> (64 - SHARD_BITS)) as usize
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct Slot {
    k: [u64; 2],
    v: [u64; 2],
}

struct Shard {
    slots: Vec<Slot>,
    mask: usize,
}

impl Shard {
    fn new(n: usize) -> Shard {
        let cap = (n * 2).next_power_of_two().max(16);
        Shard { slots: vec![Slot::default(); cap], mask: cap - 1 }
    }

    /// Inserts `k -> v` unless `k` is present; returns the value now stored.
    #[inline(always)]
    fn insert(&mut self, h: u64, k: Key, v: Key) -> Key {
        let mut i = h as usize & self.mask;
        loop {
            let s = &mut self.slots[i];
            let sk = join(s.k);
            if sk == k {
                return join(s.v);
            }
            if sk == 0 {
                s.k = split(k);
                s.v = split(v);
                return v;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn get(&self, h: u64, k: Key) -> Option<Key> {
        let mut i = h as usize & self.mask;
        loop {
            let s = &self.slots[i];
            let sk = join(s.k);
            if sk == k {
                return Some(join(s.v));
            }
            if sk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

pub struct ShardedMap {
    shards: Vec<Shard>,
}

impl ShardedMap {
    /// Builds one shard per entry list, in parallel. `lists[s]` holds only keys
    /// whose hash falls in shard `s`; for a repeated key the first value wins.
    pub fn build(lists: &[Vec<(Key, Key)>]) -> ShardedMap {
        assert_eq!(lists.len(), SHARDS);
        let shards = lists
            .par_iter()
            .map(|list| {
                let mut sh = Shard::new(list.len());
                for &(k, v) in list {
                    debug_assert!(k != 0);
                    sh.insert(keys::hash(k), k, v);
                }
                sh
            })
            .collect();
        ShardedMap { shards }
    }

    pub fn slots(&self) -> usize {
        self.shards.iter().map(|s| s.slots.len()).sum()
    }

    #[inline(always)]
    pub fn prefetch(&self, k: Key) {
        let h = keys::hash(k);
        let sh = &self.shards[shard_of(h)];
        let i = h as usize & sh.mask;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(sh.slots.as_ptr().add(i) as *const i8, _MM_HINT_T0);
        }
    }

    #[inline(always)]
    pub fn get(&self, k: Key) -> Option<Key> {
        let h = keys::hash(k);
        self.shards[shard_of(h)].get(h, k)
    }

    /// The value for `k`, or `k` itself (a person name lookup).
    #[inline(always)]
    pub fn get_or_self(&self, k: Key) -> Key {
        self.get(k).unwrap_or(k)
    }
}

/// A key and where its id is to be written.
#[derive(Clone, Copy)]
pub struct KeyRef {
    pub lo: u64,
    pub hi: u64,
    pub at: u32,
}

/// Bits of an interned id that hold the index within its shard; the shard is above them.
pub const LOCAL_BITS: u32 = 32 - SHARD_BITS;

struct SyncPtr(*mut u32);
unsafe impl Sync for SyncPtr {}

/// Gives the distinct keys of each shard dense indices in order of first
/// appearance. `jobs[j][s]` holds the keys of job `j` whose hash is in shard
/// `s` (repeats allowed); the id of each, `s << LOCAL_BITS | index`, is
/// written to `out[base[j] + at]`, and every such place must be distinct.
/// Returns the distinct keys per shard. Each shard is interned by one thread
/// with a table of 32-bit slots (tag + index) that stays in cache.
pub fn intern(jobs: &[Vec<Vec<KeyRef>>], base: &[usize], out: &mut [u32]) -> Vec<Vec<Key>> {
    let len = out.len();
    let out = SyncPtr(out.as_mut_ptr());
    (0..SHARDS)
        .into_par_iter()
        .map(|s| {
            let out = &out;
            let total: usize = jobs.iter().map(|j| j[s].len()).sum();
            assert!(total < (1 << LOCAL_BITS), "too many link IDs");
            let cap = (total * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            let ib = usize::BITS - total.leading_zeros();
            let imask = (1u32 << ib) - 1;
            let mut table = vec![0u32; cap];
            let mut uniq: Vec<Key> = Vec::with_capacity(total / 2 + 16);
            for (j, job) in jobs.iter().enumerate() {
                for r in &job[s] {
                    let k = join([r.lo, r.hi]);
                    let h = keys::hash(k);
                    // The shard is the top bits of the hash and the slot the
                    // low ones: take the tag from the middle.
                    let tag = ((h >> 20) as u32) & !imask;
                    let mut i = h as usize & mask;
                    let id = loop {
                        let t = table[i];
                        if t == 0 {
                            uniq.push(k);
                            table[i] = tag | uniq.len() as u32;
                            break uniq.len() - 1;
                        }
                        if t & !imask == tag {
                            let l = (t & imask) as usize - 1;
                            if uniq[l] == k {
                                break l;
                            }
                        }
                        i = (i + 1) & mask;
                    };
                    let at = base[j] + r.at as usize;
                    assert!(at < len);
                    unsafe { out.0.add(at).write(((s as u32) << LOCAL_BITS) | id as u32) };
                }
            }
            uniq
        })
        .collect()
}

#[inline(always)]
fn split(k: Key) -> [u64; 2] {
    [k as u64, (k >> 64) as u64]
}

#[inline(always)]
fn join(a: [u64; 2]) -> Key {
    (a[0] as u128) | ((a[1] as u128) << 64)
}
