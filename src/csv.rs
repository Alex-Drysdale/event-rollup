//! The CSV dialect of rules.md: only `\n` ends a line, every comma separates
//! fields, a field is either free of quotes or wrapped whole in one pair of
//! them, and the field count must be exact. Anything else is malformed.

use memchr::{memchr, memchr_iter};

/// Splits a line into exactly `N` fields with their wrapping quotes removed,
/// or None if the line is malformed.
#[inline]
pub fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out: [&[u8]; N] = [&[]; N];
    let mut idx = 0usize;
    let mut start = 0usize;
    let mut quotes = 0u32;
    for (i, &c) in line.iter().enumerate() {
        if c == b',' {
            if idx + 1 >= N {
                return None;
            }
            out[idx] = finish(line, start, i, quotes)?;
            idx += 1;
            start = i + 1;
            quotes = 0;
        } else if c == b'"' {
            quotes += 1;
        }
    }
    if idx + 1 != N {
        return None;
    }
    out[idx] = finish(line, start, line.len(), quotes)?;
    Some(out)
}

#[inline]
fn finish(line: &[u8], s: usize, e: usize, quotes: u32) -> Option<&[u8]> {
    match quotes {
        0 => Some(&line[s..e]),
        2 if e - s >= 2 && line[s] == b'"' && line[e - 1] == b'"' => Some(&line[s + 1..e - 1]),
        _ => None,
    }
}

/// Byte ranges of a file's data, after its first line (the header), cut into
/// pieces of about `chunk` bytes that each end just after a `\n` (or at the end).
pub fn chunks(data: &[u8], chunk: usize) -> Vec<(usize, usize)> {
    let Some(h) = memchr(b'\n', data) else {
        return Vec::new();
    };
    let n = data.len();
    let mut s = h + 1;
    let mut out = Vec::new();
    while s < n {
        let target = s + chunk.max(1);
        let e = if target >= n {
            n
        } else {
            match memchr(b'\n', &data[target - 1..]) {
                Some(p) => target - 1 + p + 1,
                None => n,
            }
        };
        out.push((s, e));
        s = e;
    }
    out
}

/// Calls `f` on every line of `buf`. A final line without `\n` counts.
#[inline]
pub fn for_lines(buf: &[u8], mut f: impl FnMut(&[u8])) {
    let mut s = 0;
    for p in memchr_iter(b'\n', buf) {
        f(&buf[s..p]);
        s = p + 1;
    }
    if s < buf.len() {
        f(&buf[s..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect() {
        assert_eq!(split::<2>(b"a,b"), Some([&b"a"[..], b"b"]));
        assert_eq!(split::<2>(b"\"a\",\"\""), Some([&b"a"[..], b""]));
        assert_eq!(split::<2>(b"a,b\r"), Some([&b"a"[..], b"b\r"]));
        assert_eq!(split::<2>(b""), None);
        assert_eq!(split::<2>(b"a"), None);
        assert_eq!(split::<2>(b"a,b,c"), None);
        assert_eq!(split::<2>(b"a,b\""), None);
        assert_eq!(split::<2>(b"a,\""), None);
        assert_eq!(split::<2>(b"a,\"\"\""), None);
        assert_eq!(split::<2>(b"a,x\"y\""), None);
        assert_eq!(split::<1>(b""), None);
    }

    #[test]
    fn chunking() {
        let data = b"h\nl1\nline2\nl3\nlast";
        for c in 1..20 {
            let mut lines = Vec::new();
            for (s, e) in chunks(data, c) {
                for_lines(&data[s..e], |l| lines.push(l.to_vec()));
            }
            assert_eq!(lines, vec![b"l1".to_vec(), b"line2".to_vec(), b"l3".to_vec(), b"last".to_vec()], "chunk {c}");
        }
        assert!(chunks(b"header only", 4).is_empty());
        assert!(chunks(b"header\n", 4).is_empty());
    }
}
