//! String keys packed into a u128.
//!
//! A value of at most `inline_max()` bytes (15 by default) is stored inline: its
//! bytes left-aligned in big-endian order and its length in the lowest byte, so
//! comparing two inline keys as integers compares the strings byte by byte
//! (a proper prefix sorts first). Longer values are interned in a global table
//! and stored as `index << 8 | 0xFF`; `cmp` and `bytes` resolve them.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Mutex;

pub type Key = u128;

const LONG_TAG: u128 = 0xFF;
static INLINE_MAX: AtomicUsize = AtomicUsize::new(15);

struct Interner {
    map: HashMap<Vec<u8>, u32>,
    strings: Vec<Vec<u8>>,
}

static LONG: Mutex<Option<Interner>> = Mutex::new(None);

/// Testing knob: force values longer than `n` bytes onto the interned path.
pub fn set_inline_max(n: usize) {
    INLINE_MAX.store(n.min(15), AtomicOrdering::Relaxed);
}

#[inline]
pub fn inline_max() -> usize {
    INLINE_MAX.load(AtomicOrdering::Relaxed)
}

#[inline]
pub fn is_inline(k: Key) -> bool {
    (k & 0xFF) != LONG_TAG
}

#[inline]
pub fn pack(b: &[u8]) -> Key {
    if b.len() <= inline_max() {
        let mut buf = [0u8; 16];
        buf[..b.len()].copy_from_slice(b);
        buf[15] = b.len() as u8;
        u128::from_be_bytes(buf)
    } else {
        intern(b)
    }
}

#[cold]
fn intern(b: &[u8]) -> Key {
    let mut guard = LONG.lock().unwrap();
    let it = guard.get_or_insert_with(|| Interner { map: HashMap::new(), strings: Vec::new() });
    let idx = match it.map.get(b) {
        Some(&i) => i,
        None => {
            let i = it.strings.len() as u32;
            it.strings.push(b.to_vec());
            it.map.insert(b.to_vec(), i);
            i
        }
    };
    ((idx as u128) << 8) | LONG_TAG
}

/// Appends the key's bytes to `out`.
pub fn write_bytes(k: Key, out: &mut Vec<u8>) {
    if is_inline(k) {
        let buf = k.to_be_bytes();
        let len = buf[15] as usize;
        out.extend_from_slice(&buf[..len]);
    } else {
        let guard = LONG.lock().unwrap();
        let it = guard.as_ref().expect("long key without interner");
        out.extend_from_slice(&it.strings[(k >> 8) as usize]);
    }
}

pub fn to_vec(k: Key) -> Vec<u8> {
    let mut v = Vec::new();
    write_bytes(k, &mut v);
    v
}

/// Byte-wise comparison of the strings two keys stand for.
#[inline]
pub fn cmp(a: Key, b: Key) -> Ordering {
    if is_inline(a) && is_inline(b) {
        a.cmp(&b)
    } else {
        cmp_slow(a, b)
    }
}

#[cold]
fn cmp_slow(a: Key, b: Key) -> Ordering {
    to_vec(a).cmp(&to_vec(b))
}

/// Hash of a key: a folded 128-bit multiply, twice. Partitioning uses the top
/// bits, hash tables the low bits.
#[inline]
pub fn hash(k: Key) -> u64 {
    let lo = k as u64;
    let hi = (k >> 64) as u64;
    let m = ((lo ^ 0x243F_6A88_85A3_08D3) as u128).wrapping_mul((hi ^ 0x1319_8A2E_0370_7344) as u128);
    let h = (m as u64) ^ ((m >> 64) as u64);
    let m2 = ((h ^ 0xA409_3822_299F_31D0) as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15u128);
    (m2 as u64) ^ ((m2 >> 64) as u64)
}

/// A `Hasher` for maps keyed by `Key`.
#[derive(Default, Clone, Copy)]
pub struct KeyHasher(u64);

impl std::hash::Hasher for KeyHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Only used for non-u128 keys; FNV-style.
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x100_0000_01B3);
        }
    }
    #[inline]
    fn write_u128(&mut self, k: u128) {
        self.0 = hash(k);
    }
}

pub type KeyBuild = std::hash::BuildHasherDefault<KeyHasher>;
pub type KeyMap<V> = HashMap<Key, V, KeyBuild>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_matches_bytes() {
        let words: Vec<&[u8]> = vec![b"", b"a", b"a\0", b"a\x01", b"ab", b"b", b"U1", b"u1", b" u1", b"u1 ", b"user_id_a\t"];
        for x in &words {
            for y in &words {
                assert_eq!(cmp(pack(x), pack(y)), x.cmp(y), "{x:?} {y:?}");
            }
        }
    }
}
