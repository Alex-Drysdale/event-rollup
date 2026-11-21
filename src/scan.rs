//! Chunk scanner for the CSV dialect (see csv.rs for the rules). Instead of
//! walking bytes, it builds 64-bit masks of `\n`, `,` and `"` for each 64-byte
//! block (one AVX-512 compare each) and visits only the separators. Quotes are
//! checked per line: a line with no quote needs no field checks; otherwise
//! every field must be wrapped (first and last byte a quote, length >= 2) or
//! quote-free, and the line's quote count must equal two per wrapped field,
//! which rules out a quote anywhere else.

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

/// Field byte ranges `(start, end)` into the file, quotes removed.
pub type Fields<const N: usize> = [(usize, usize); N];

/// Calls `emit` for every well-formed line with exactly `N` fields in
/// `file[start..end]`. `start` must be a line start and `end` a line end (just
/// after a `\n`, or the end of the file). A last line without `\n` counts.
#[inline(always)]
pub fn scan<const N: usize>(file: &[u8], start: usize, end: usize, mut emit: impl FnMut(&Fields<N>)) {
    debug_assert!(end <= file.len());
    let mut seps = [0usize; N];
    let mut nf = 0usize;
    let mut line_start = start;
    let mut q_block = 0u32; // quotes in [start, b)
    let mut q_line = 0u32; // quotes in [start, line_start)
    let mut b = start;
    let mut pad: [u8; 64];
    while b < end {
        let (nl, cm, qt) = if b + 64 <= end {
            unsafe { masks(file.as_ptr().add(b)) }
        } else {
            let n = end - b;
            pad = [0u8; 64];
            pad[..n].copy_from_slice(&file[b..end]);
            unsafe { masks(pad.as_ptr()) }
        };
        let mut sep = nl | cm;
        while sep != 0 {
            let off = sep.trailing_zeros();
            let pos = b + off as usize;
            if (nl >> off) & 1 == 0 {
                if nf < N - 1 {
                    seps[nf] = pos;
                }
                nf += 1;
            } else {
                let below = u64::MAX >> (63 - off); // bits 0..=off
                let q_at = q_block + (qt & below).count_ones();
                if nf == N - 1 {
                    seps[N - 1] = pos;
                    finish(file, line_start, &seps, q_at - q_line, &mut emit);
                }
                line_start = pos + 1;
                nf = 0;
                q_line = q_at;
            }
            sep &= sep - 1;
        }
        q_block += qt.count_ones();
        b += 64;
    }
    if line_start < end && nf == N - 1 {
        seps[N - 1] = end;
        finish(file, line_start, &seps, q_block - q_line, &mut emit);
    }
}

#[inline(always)]
fn finish<const N: usize>(
    file: &[u8],
    line_start: usize,
    seps: &[usize; N],
    quotes: u32,
    emit: &mut impl FnMut(&Fields<N>),
) {
    // Branch-free: which fields are quoted is random, so branches on it would
    // mispredict. Data lines start after the header, so `s >= 1` and `e - 1`
    // is in bounds; `s` itself can be the file's length for an empty last field.
    let mut f = [(0usize, 0usize); N];
    let mut s = line_start;
    let mut expected = 0u32;
    let mut bad = false;
    let last = file.len() - 1;
    for i in 0..N {
        let e = seps[i];
        let len = e - s;
        let nonempty = len >= 1;
        let fq = nonempty & (file[s.min(last)] == b'"');
        let lq = nonempty & (file[e.max(1) - 1] == b'"');
        bad |= (fq != lq) | (fq & (len < 2));
        expected += 2 * fq as u32;
        f[i] = (s + fq as usize, e - fq as usize);
        s = e + 1;
    }
    if bad | (expected != quotes) {
        return;
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
