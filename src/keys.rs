//! String keys packed into integers: `Key` (u128) for the lookup tables and
//! `Id` (u64, further down) for the IDs carried in records.
//!
//! A value of at most `inline_max()` bytes (15 by default) is stored inline: its
//! bytes left-aligned in big-endian order and its length in the lowest byte, so
//! comparing two inline keys as integers compares the strings byte by byte
//! (a proper prefix sorts first). Longer values are interned in a global table
//! and stored as `index << 8 | 0xFF`. The lookup tables only test keys for
//! equality.

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
    ID_INLINE_MAX.store(n.min(ID_MAX_LEN), AtomicOrdering::Relaxed);
}

#[inline]
pub fn inline_max() -> usize {
    INLINE_MAX.load(AtomicOrdering::Relaxed)
}

#[inline]
#[cfg(test)]
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

fn intern_in(table: &Mutex<Option<Interner>>, b: &[u8]) -> u32 {
    let mut guard = table.lock().unwrap();
    let it = guard.get_or_insert_with(|| Interner { map: HashMap::new(), strings: Vec::new() });
    match it.map.get(b) {
        Some(&i) => i,
        None => {
            let i = u32::try_from(it.strings.len()).expect("too many long IDs");
            it.strings.push(b.to_vec());
            it.map.insert(b.to_vec(), i);
            i
        }
    }
}

#[cold]
fn intern(b: &[u8]) -> Key {
    ((intern_in(&LONG, b) as u128) << 8) | LONG_TAG
}

/// Appends the key's bytes to `out`.
#[cfg(test)]
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

#[cfg(test)]
pub fn to_vec(k: Key) -> Vec<u8> {
    let mut v = Vec::new();
    write_bytes(k, &mut v);
    v
}

/// Byte-wise comparison of the strings two keys stand for.
#[inline]
#[cfg(test)]
pub fn cmp(a: Key, b: Key) -> Ordering {
    if is_inline(a) && is_inline(b) {
        a.cmp(&b)
    } else {
        cmp_slow(a, b)
    }
}

#[cold]
#[cfg(test)]
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

// ---------------------------------------------------------------------------
// 64-bit IDs

/// An ID in 64 bits. A value of at most `id_inline_max()` bytes (10 by
/// default) whose bytes are all in 0x20..=0x3F or 0x60..=0x7F (digits,
/// lower-case letters, space and some punctuation) is stored inline: each byte
/// becomes a 6-bit code that keeps the byte order (bits 0-4 and bit 6 of the
/// byte), the codes are left-aligned from bit 63 down and the length is in
/// the low 4 bits. Integer order of two inline IDs is then byte-wise order of
/// the strings: the codes compare like the bytes, and when one string is the
/// other plus trailing spaces (code 0, like the padding) the length decides,
/// shorter first. The encoding is one-to-one, so equal integers mean equal
/// strings. Any other value is interned and stored as `index << 4 | 0xF`;
/// `id_cmp` and `id_write_bytes` resolve those. The empty string is 0.
pub type Id = u64;

pub const ID_MAX_LEN: usize = 10;
const ID_LONG: u64 = 0xF;
static ID_INLINE_MAX: AtomicUsize = AtomicUsize::new(ID_MAX_LEN);
static LONG_ID: Mutex<Option<Interner>> = Mutex::new(None);

#[inline]
pub fn id_inline_max() -> usize {
    ID_INLINE_MAX.load(AtomicOrdering::Relaxed)
}

#[inline]
pub fn id_is_inline(k: Id) -> bool {
    (k & 0xF) != ID_LONG
}

/// The reference packer (`scan::pack_id_at` is the fast one).
#[inline]
pub fn pack_id(b: &[u8]) -> Id {
    if b.len() <= id_inline_max() && b.iter().all(|&c| c & 0xA0 == 0x20) {
        let mut k = b.len() as u64;
        for (i, &c) in b.iter().enumerate() {
            let code = (c & 0x1F) | ((c & 0x40) >> 1);
            k |= (code as u64) << (58 - 6 * i);
        }
        k
    } else {
        intern_id(b)
    }
}

#[cold]
#[inline(never)]
fn intern_id(b: &[u8]) -> Id {
    ((intern_in(&LONG_ID, b) as u64) << 4) | ID_LONG
}

/// Appends the ID's bytes to `out`.
pub fn id_write_bytes(k: Id, out: &mut Vec<u8>) {
    if id_is_inline(k) {
        let len = (k & 0xF) as usize;
        for i in 0..len {
            let code = (k >> (58 - 6 * i)) as u8 & 0x3F;
            out.push(0x20 | (code & 0x1F) | ((code & 0x20) << 1));
        }
    } else {
        let guard = LONG_ID.lock().unwrap();
        let it = guard.as_ref().expect("long ID without interner");
        out.extend_from_slice(&it.strings[(k >> 4) as usize]);
    }
}

pub fn id_to_vec(k: Id) -> Vec<u8> {
    let mut v = Vec::new();
    id_write_bytes(k, &mut v);
    v
}

/// Byte-wise comparison of the strings two IDs stand for.
#[inline]
pub fn id_cmp(a: Id, b: Id) -> Ordering {
    if id_is_inline(a) && id_is_inline(b) {
        a.cmp(&b)
    } else {
        id_cmp_slow(a, b)
    }
}

#[cold]
#[inline(never)]
fn id_cmp_slow(a: Id, b: Id) -> Ordering {
    id_to_vec(a).cmp(&id_to_vec(b))
}

/// Hash of an ID: two folded multiplies. Partitioning uses the top bits, hash
/// tables the low bits.
#[inline(always)]
pub fn id_hash(k: Id) -> u64 {
    let m = ((k ^ 0x243F_6A88_85A3_08D3) as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15u128);
    let h = (m as u64) ^ ((m >> 64) as u64);
    let m2 = ((h ^ 0xA409_3822_299F_31D0) as u128).wrapping_mul(0xD6E8_FEB8_6659_FD93u128);
    (m2 as u64) ^ ((m2 >> 64) as u64)
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// Serialises the tests that change the inline limit.
    pub static LIMIT: Mutex<()> = Mutex::new(());

    /// Strings over the bytes at the edges of the inline alphabet, of every
    /// length around the inline limit, with prefixes, trailing spaces and case
    /// variants; sorted and distinct.
    pub fn id_cases() -> Vec<Vec<u8>> {
        let edge: &[u8] =
            &[0x00, 0x1F, 0x20, 0x21, 0x2F, 0x30, 0x39, 0x3F, 0x40, 0x41, 0x5A, 0x5F, 0x60, 0x61, 0x7A, 0x7F, 0x80, 0xA0, 0xFF];
        let mut cases: Vec<Vec<u8>> = vec![Vec::new()];
        for &c in edge {
            cases.push(vec![c]);
            for &d in edge {
                cases.push(vec![c, d]);
                cases.push(vec![b'u', c, d]);
            }
        }
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut rnd = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let lower: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
        for len in 0..=17usize {
            for round in 0..24 {
                let mut v: Vec<u8> = (0..len).map(|_| lower[(rnd() % 36) as usize]).collect();
                if round % 4 == 1 && len > 0 {
                    let at = (rnd() % len as u64) as usize;
                    v[at] = edge[(rnd() % edge.len() as u64) as usize];
                }
                if round % 4 == 2 {
                    v = (0..len).map(|_| [b' ', b'0', b'a', b'z', 0x7F, 0x3F, 0x60][(rnd() % 7) as usize]).collect();
                }
                for cut in 0..=len {
                    cases.push(v[..cut].to_vec());
                }
                let mut sp = v.clone();
                sp.push(b' ');
                cases.push(sp.clone());
                sp.push(b' ');
                cases.push(sp);
                cases.push(v.to_ascii_uppercase());
                cases.push(v);
            }
        }
        for s in [
            &b"u1"[..], b"U1", b" u1", b"u1 ", b"u1  ", b"u1\r", b"u4k2nn2cj", b"U4K2NN2CJ", b"e4ajy5dva", b"c_332", b"p42104",
            b"\"u1\"", b"u1\0",
        ] {
            cases.push(s.to_vec());
        }
        cases.sort();
        cases.dedup();
        cases
    }

    fn check_ids(limit: usize) {
        set_inline_max(limit);
        let cases = id_cases();
        let ids: Vec<Id> = cases.iter().map(|c| pack_id(c)).collect();
        for (c, &k) in cases.iter().zip(&ids) {
            assert_eq!(id_to_vec(k), *c, "round trip {c:?}");
            assert_eq!(pack_id(c), k, "stable {c:?}");
            assert_eq!(k == 0, c.is_empty(), "zero {c:?}");
            let inline =
                c.len() <= limit.min(ID_MAX_LEN) && c.iter().all(|&b| (0x20..=0x3F).contains(&b) || (0x60..=0x7F).contains(&b));
            assert_eq!(id_is_inline(k), inline, "inline {c:?}");
        }
        // `cases` is sorted and distinct: the IDs must be too, under `id_cmp`.
        for i in 0..cases.len() {
            for j in (i % 5..cases.len()).step_by(5) {
                assert_eq!(ids[i] == ids[j], i == j, "{:?} {:?}", cases[i], cases[j]);
                assert_eq!(id_cmp(ids[i], ids[j]), i.cmp(&j), "{:?} {:?}", cases[i], cases[j]);
            }
            if i > 0 {
                assert_eq!(id_cmp(ids[i - 1], ids[i]), Ordering::Less, "{:?} {:?}", cases[i - 1], cases[i]);
                assert_eq!(id_cmp(ids[i], ids[i - 1]), Ordering::Greater, "{:?} {:?}", cases[i - 1], cases[i]);
            }
        }
    }

    #[test]
    fn id_matches_bytes() {
        let _guard = LIMIT.lock().unwrap();
        for limit in [10, 3, 0, 9] {
            check_ids(limit);
        }
        set_inline_max(15);
    }

    #[test]
    fn order_matches_bytes() {
        let _guard = LIMIT.lock().unwrap();
        let words: Vec<&[u8]> = vec![b"", b"a", b"a\0", b"a\x01", b"ab", b"b", b"U1", b"u1", b" u1", b"u1 ", b"user_id_a\t"];
        for x in &words {
            for y in &words {
                assert_eq!(cmp(pack(x), pack(y)), x.cmp(y), "{x:?} {y:?}");
            }
        }
    }
}
