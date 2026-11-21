//! Chunk scanner for the CSV dialect (see csv.rs for the rules). It works a
//! line at a time on bit masks: the 128 bytes from the start of a line are
//! compared with `\n`, `,` and `"` (two AVX-512 compares each), the first
//! newline bit ends the line, the comma bits below it give the fields, and
//! the quotes are checked with mask arithmetic alone. Longer lines, and the
//! last few of a file, take a slower path with the same rules.
//!
//! Quotes: number the quotes of a line from 1. The line is well-formed iff
//! every odd quote is the first byte of a field, every even quote the last
//! byte of a field, there is no comma between an odd quote and the next one,
//! and their number is even. Then each odd quote and the next wrap one whole
//! field, and no field has a quote anywhere else.

use crate::keys::{self, Key};

#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
    unsafe {
        use std::arch::x86_64::*;
        let v = _mm512_loadu_si512(p as *const _);
        let nl = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8));
        let cm = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8));
        let qt = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8));
        (nl, cm, qt)
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512bw")))]
    unsafe {
        use std::arch::x86_64::*;
        let half = |q: *const u8| {
            let v = _mm256_loadu_si256(q as *const __m256i);
            let nl = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'\n' as i8))) as u32 as u64;
            let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b',' as i8))) as u32 as u64;
            let qt = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'"' as i8))) as u32 as u64;
            (nl, cm, qt)
        };
        let (a0, b0, c0) = half(p);
        let (a1, b1, c1) = half(p.add(32));
        (a0 | (a1 << 32), b0 | (b1 << 32), c0 | (c1 << 32))
    }
    #[cfg(not(all(target_arch = "x86_64", any(target_feature = "avx2", target_feature = "avx512bw"))))]
    unsafe {
        let (mut nl, mut cm, mut qt) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let c = *p.add(i);
            nl |= ((c == b'\n') as u64) << i;
            cm |= ((c == b',') as u64) << i;
            qt |= ((c == b'"') as u64) << i;
        }
        (nl, cm, qt)
    }
}

/// How far ahead of the scan to prefetch input.
const PREFETCH: usize = 1024;

/// The masks of the 128 bytes at `p`.
#[inline(always)]
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    unsafe {
        let (a0, b0, c0) = masks(p);
        let (a1, b1, c1) = masks(p.add(64));
        (a0 as u128 | (a1 as u128) << 64, b0 as u128 | (b1 as u128) << 64, c0 as u128 | (c1 as u128) << 64)
    }
}

/// Bit `i` of the result is the parity of bits `0..=i` of `x`.
#[inline(always)]
fn prefix_xor64(x: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "pclmulqdq"))]
    unsafe {
        use std::arch::x86_64::*;
        let r = _mm_clmulepi64_si128(_mm_set_epi64x(0, x as i64), _mm_set1_epi8(-1), 0);
        _mm_cvtsi128_si64(r) as u64
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "pclmulqdq")))]
    {
        let mut x = x;
        x ^= x << 1;
        x ^= x << 2;
        x ^= x << 4;
        x ^= x << 8;
        x ^= x << 16;
        x ^= x << 32;
        x
    }
}

#[inline(always)]
fn prefix_xor(x: u128) -> u128 {
    let lo = prefix_xor64(x as u64);
    let hi = prefix_xor64((x >> 64) as u64) ^ 0u64.wrapping_sub(lo >> 63);
    lo as u128 | (hi as u128) << 64
}

/// The bits of `x` at the set bits of `m`, packed from bit 0 up.
#[inline(always)]
fn pext64(x: u64, m: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    unsafe {
        std::arch::x86_64::_pext_u64(x, m)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let (mut m, mut out, mut k) = (m, 0u64, 0);
        while m != 0 {
            out |= ((x >> m.trailing_zeros()) & 1) << k;
            k += 1;
            m &= m - 1;
        }
        out
    }
}

/// Field byte ranges `(start, end)` into the file, quotes removed.
pub type Fields<const N: usize> = [(usize, usize); N];

/// Calls `emit` for every well-formed line with exactly `N` fields in
/// `file[start..end]`. `start` must be a line start and `end` a line end (just
/// after a `\n`, or the end of the file). A last line without `\n` counts.
#[inline(always)]
pub fn scan<const N: usize>(file: &[u8], start: usize, end: usize, mut emit: impl FnMut(&Fields<N>)) {
    assert!(end <= file.len() && N >= 1 && N <= 64);
    let mut ls = start;
    while ls < end {
        if ls + 128 <= file.len() {
            #[cfg(target_arch = "x86_64")]
            if ls + PREFETCH < end {
                unsafe {
                    use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                    _mm_prefetch(file.as_ptr().add(ls + PREFETCH) as *const i8, _MM_HINT_T0);
                }
            }
            let (nl, cm, qt) = unsafe { masks128(file.as_ptr().add(ls)) };
            if nl != 0 {
                let len = nl.trailing_zeros();
                line(ls, len, cm, qt, &mut emit);
                ls += len as usize + 1;
                continue;
            }
        }
        ls = slow_line(file, ls, end, &mut emit);
    }
}

/// A line that is long, or within 128 bytes of the end of the file. Returns
/// the start of the next line.
#[cold]
#[inline(never)]
fn slow_line<const N: usize>(file: &[u8], ls: usize, end: usize, emit: &mut impl FnMut(&Fields<N>)) -> usize {
    let rest = &file[ls..end];
    let len = memchr::memchr(b'\n', rest).unwrap_or(rest.len());
    if len < 128 {
        let mut pad = [0u8; 128];
        pad[..len].copy_from_slice(&rest[..len]);
        let (_, cm, qt) = unsafe { masks128(pad.as_ptr()) };
        line(ls, len as u32, cm, qt, emit);
    } else if let Some(fields) = crate::csv::split::<N>(&rest[..len]) {
        let base = file.as_ptr() as usize;
        let mut f = [(0usize, 0usize); N];
        for (r, x) in f.iter_mut().zip(fields) {
            let s = x.as_ptr() as usize - base;
            *r = (s, s + x.len());
        }
        emit(&f);
    }
    ls + len + 1
}

/// The line of `len < 128` bytes at `ls`, given the comma and quote masks of
/// (at least) its bytes.
#[inline(always)]
fn line<const N: usize>(ls: usize, len: u32, cm: u128, qt: u128, emit: &mut impl FnMut(&Fields<N>)) {
    let below = (1u128 << len) - 1;
    let c = cm & below;
    let q = qt & below;
    // An empty line is malformed whatever `N` is.
    if c.count_ones() != N as u32 - 1 || len == 0 {
        return;
    }
    // Bit `i` of `wrapped`: field `i` is wrapped in quotes.
    let mut wrapped = 0u64;
    if q != 0 {
        let inside = prefix_xor(q); // from each odd quote up to the next quote
        let starts = (c << 1) | 1;
        let ends = (c | (1u128 << len)) >> 1;
        let odd = q & inside;
        let even = q & !inside;
        if (inside >> 127) != 0 || (odd & !starts) != 0 || (even & !ends) != 0 || (c & inside) != 0 {
            return;
        }
        let lo = starts as u64;
        wrapped = pext64(odd as u64, lo) | pext64((odd >> 64) as u64, (starts >> 64) as u64) << lo.count_ones();
    }
    let mut f = [(0usize, 0usize); N];
    let mut rest = c;
    let mut s = ls;
    for (i, r) in f.iter_mut().enumerate() {
        let e = ls + if i == N - 1 { len } else { rest.trailing_zeros() } as usize;
        rest &= rest.wrapping_sub(1);
        let w = (wrapped >> i) as usize & 1;
        *r = (s + w, e - w);
        s = e + 1;
    }
    emit(&f);
}

/// `keys::pack` of `file[s..e]`, reading 16 bytes at once when it can.
#[inline(always)]
pub fn pack_at(file: &[u8], s: usize, e: usize) -> Key {
    let len = e - s;
    if len <= keys::inline_max() && s + 16 <= file.len() {
        let raw = u128::from_be_bytes(file[s..s + 16].try_into().unwrap());
        let mask = if len == 0 { 0 } else { !0u128 << (8 * (16 - len)) };
        (raw & mask) | len as u128
    } else {
        keys::pack(&file[s..e])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines<const N: usize>(text: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        scan::<N>(text, 0, text.len(), |f| out.push(f.iter().map(|&(s, e)| text[s..e].to_vec()).collect()));
        out
    }

    #[test]
    fn matches_split() {
        let cases: Vec<&[u8]> = vec![
            b"a,b\n", b"\"a\",\"\"\n", b"a,b\r\n", b"\n", b"a\n", b"a,b,c\n", b"a,b\"\n", b"a,\"\n",
            b"a,\"\"\"\n", b"a,x\"y\"\n", b"\"\",\"\"\n", b"\"a\"b,c\n", b",\n", b"\"a,b\"\n", b"a,b",
        ];
        for c in &cases {
            let want: Vec<Vec<Vec<u8>>> = crate::csv::split::<2>(c.strip_suffix(b"\n").unwrap_or(c))
                .map(|f| vec![f.iter().map(|x| x.to_vec()).collect()])
                .unwrap_or_default();
            assert_eq!(lines::<2>(c), want, "{:?}", String::from_utf8_lossy(c));
        }
        // Long lines across block boundaries, with quotes and \r.
        let mut text = Vec::new();
        let mut want = Vec::new();
        for i in 0..500 {
            let a = format!("{}", "x".repeat(i % 97));
            let b = format!("\"{}\"", "y".repeat(i % 61));
            let line = format!("{a},{b}{}\n", if i % 7 == 0 { "\r" } else { "" });
            text.extend_from_slice(line.as_bytes());
            let mut bb = "y".repeat(i % 61);
            if i % 7 == 0 {
                // `"yyy"\r` is not wrapped whole: malformed.
                continue;
            }
            bb.truncate(i % 61);
            want.push(vec![a.into_bytes(), bb.into_bytes()]);
            if i % 5 == 0 {
                text.extend_from_slice(b"bad\"line,x\n\n");
            }
        }
        assert_eq!(lines::<2>(&text), want);
    }

    /// Random text over a small alphabet, against the reference splitter.
    #[test]
    fn random_lines() {
        let alphabet: &[u8] = b"ab,,\"\"\n\n\r ";
        let mut seed = 0x1234_5678_9ABC_DEF1u64;
        let mut rnd = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..20000 {
            let n = (rnd() % 400) as usize;
            // Mostly short lines; sometimes long runs without a newline.
            let long = round % 7 == 0;
            let text: Vec<u8> = (0..n)
                .map(|_| {
                    let c = alphabet[(rnd() % alphabet.len() as u64) as usize];
                    if long && c == b'\n' && rnd() % 8 != 0 { b'x' } else { c }
                })
                .collect();
            for nf in [1usize, 2, 3, 4, 7] {
                let mut want: Vec<Vec<Vec<u8>>> = Vec::new();
                crate::csv::for_lines(&text, |l| {
                    let fields: Option<Vec<Vec<u8>>> = match nf {
                        1 => crate::csv::split::<1>(l).map(|f| f.iter().map(|x| x.to_vec()).collect()),
                        2 => crate::csv::split::<2>(l).map(|f| f.iter().map(|x| x.to_vec()).collect()),
                        3 => crate::csv::split::<3>(l).map(|f| f.iter().map(|x| x.to_vec()).collect()),
                        4 => crate::csv::split::<4>(l).map(|f| f.iter().map(|x| x.to_vec()).collect()),
                        _ => crate::csv::split::<7>(l).map(|f| f.iter().map(|x| x.to_vec()).collect()),
                    };
                    want.extend(fields);
                });
                let got = match nf {
                    1 => lines::<1>(&text),
                    2 => lines::<2>(&text),
                    3 => lines::<3>(&text),
                    4 => lines::<4>(&text),
                    _ => lines::<7>(&text),
                };
                assert_eq!(got, want, "{nf} fields: {:?}", String::from_utf8_lossy(&text));
            }
        }
    }

    #[test]
    fn pack_matches() {
        let text = b"abc,,0123456789abcde,0123456789abcdef,xyz";
        let mut s = 0;
        for (i, &c) in text.iter().enumerate() {
            if c == b',' || i == text.len() - 1 {
                let e = if c == b',' { i } else { i + 1 };
                assert_eq!(pack_at(text, s, e), keys::pack(&text[s..e]));
                s = i + 1;
            }
        }
    }
}
