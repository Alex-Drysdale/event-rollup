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

/// Dense ids for distinct keys: shard `s` owns ids `offsets[s]..offsets[s+1]`.
pub struct Interner {
    pub map: ShardedMap,
    /// The key of each id.
    pub keys: Vec<Key>,
    /// Shard `s` owns ids `offsets[s]..offsets[s + 1]`.
    pub offsets: Vec<usize>,
}

impl Interner {
    /// `lists[s]`: keys (repeats allowed) whose hash is in shard `s`.
    pub fn build(lists: &[Vec<Key>]) -> Interner {
        assert_eq!(lists.len(), SHARDS);
        let built: Vec<(Shard, Vec<Key>)> = lists
            .par_iter()
            .map(|list| {
                let mut sh = Shard::new(list.len());
                let mut uniq = Vec::new();
                for &k in list {
                    let next = uniq.len() as u128;
                    if sh.insert(keys::hash(k), k, next) == next {
                        uniq.push(k);
                    }
                }
                (sh, uniq)
            })
            .collect();
        let mut offsets = Vec::with_capacity(SHARDS);
        let mut offset = 0u128;
        let mut keys_all = Vec::new();
        for (_, uniq) in &built {
            offsets.push(offset);
            offset += uniq.len() as u128;
            keys_all.extend_from_slice(uniq);
        }
        let offsets_usize: Vec<usize> = offsets.iter().map(|&o| o as usize).collect();
        // Turn shard-local ids into global ones.
        let shards = built
            .into_par_iter()
            .zip(offsets)
            .map(|((mut sh, _), off)| {
                if off != 0 {
                    for s in sh.slots.iter_mut() {
                        if join(s.k) != 0 {
                            s.v = split(join(s.v) + off);
                        }
                    }
                }
                sh
            })
            .collect();
        let mut bounds: Vec<usize> = offsets_usize;
        bounds.push(keys_all.len());
        Interner { map: ShardedMap { shards }, keys: keys_all, offsets: bounds }
    }

    #[inline(always)]
    pub fn id(&self, k: Key) -> u32 {
        self.map.get(k).expect("interned key") as u32
    }
}

#[inline(always)]
fn split(k: Key) -> [u64; 2] {
    [k as u64, (k >> 64) as u64]
}

#[inline(always)]
fn join(a: [u64; 2]) -> Key {
    (a[0] as u128) | ((a[1] as u128) << 64)
}
