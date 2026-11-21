//! Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, F3 epoch
//! milliseconds as ASCII digits. Returns milliseconds since the epoch, UTC, or
//! None for anything else (including an F3 value too big for an i64, which can
//! only be outside every window anyway).

pub const DAY_MS: i64 = 86_400_000;

#[inline(always)]
pub fn parse(b: &[u8]) -> Option<i64> {
    let n = b.len();
    // F1 and F2 have '-' at index 4, which no F3 (all digits) can have.
    if n >= 20 && b[4] == b'-' {
        if n == 20 {
            if b[19] != b'Z' {
                return None;
            }
            return datetime(b);
        }
        if n != 25 {
            return None;
        }
        let sign = match b[19] {
            b'+' => 1i64,
            b'-' => -1i64,
            _ => return None,
        };
        if b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)? as i64;
        let om = d2(b, 23)? as i64;
        if oh > 23 || om > 59 {
            return None;
        }
        let local = datetime(b)?;
        return Some(local - sign * (oh * 60 + om) * 60_000);
    }
    parse_f3(b)
}

/// Digits only, any number of leading zeros; None if empty, not all ASCII
/// digits, or too big for an i64.
#[inline(always)]
fn parse_f3(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    if b.len() <= 18 {
        for &c in b {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v * 10 + d as u64;
        }
        return Some(v as i64);
    }
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    if v > i64::MAX as u64 { None } else { Some(v as i64) }
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<u32> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a > 9 || c > 9 { None } else { Some(a as u32 * 10 + c as u32) }
}

#[inline]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline]
fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    }
}

pub const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year / 400 } else { (year - 399) / 400 };
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// The first 19 bytes, `YYYY-MM-DDTHH:MM:SS`, as epoch milliseconds.
#[inline(always)]
fn datetime(b: &[u8]) -> Option<i64> {
    let b: &[u8; 19] = b[..19].try_into().ok()?;
    let dg = |i: usize| b[i].wrapping_sub(b'0') as u32;
    let x = [dg(0), dg(1), dg(2), dg(3), dg(5), dg(6), dg(8), dg(9), dg(11), dg(12), dg(14), dg(15), dg(17), dg(18)];
    let mut bad = (b[4] != b'-') | (b[7] != b'-') | (b[10] != b'T') | (b[13] != b':') | (b[16] != b':');
    for &v in &x {
        bad |= v > 9;
    }
    let y = x[0] * 1000 + x[1] * 100 + x[2] * 10 + x[3];
    let mo = x[4] * 10 + x[5];
    let d = x[6] * 10 + x[7];
    let h = x[8] * 10 + x[9];
    let mi = x[10] * 10 + x[11];
    let s = x[12] * 10 + x[13];
    bad |= (mo.wrapping_sub(1) > 11) | (d == 0) | (h > 23) | (mi > 59) | (s > 59);
    if bad {
        return None;
    }
    let days = if (TABLE_FIRST_YEAR..TABLE_FIRST_YEAR + TABLE_YEARS).contains(&y) {
        // Days to the first of this month and of the next one.
        let i = ((y - TABLE_FIRST_YEAR) * 12 + mo - 1) as usize;
        let (first, next) = (MONTH_STARTS[i], MONTH_STARTS[i + 1]);
        if d as i32 > next - first {
            return None;
        }
        (first + d as i32 - 1) as i64
    } else {
        if d > days_in_month(y as i64, mo) {
            return None;
        }
        days_from_civil(y as i64, mo as i64, d as i64)
    };
    Some((days * 86_400 + (h * 3600 + mi * 60 + s) as i64) * 1000)
}

const TABLE_FIRST_YEAR: u32 = 1900;
const TABLE_YEARS: u32 = 300;

/// Days since the epoch of the first of each month, 1900-01 to 2200-01.
static MONTH_STARTS: [i32; TABLE_YEARS as usize * 12 + 1] = {
    let mut t = [0i32; TABLE_YEARS as usize * 12 + 1];
    let mut i = 0;
    while i < t.len() {
        let y = (TABLE_FIRST_YEAR as usize + i / 12) as i64;
        let m = (i % 12 + 1) as i64;
        t[i] = days_from_civil(y, m, 1) as i32;
        i += 1;
    }
    t
};

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse(b"2026-10-04T01:30:00+02:00"), parse(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse(b"2026-10-03T10:00:00-00:00"), parse(b"2026-10-03T10:00:00Z"));
        assert!(parse(b"2026-10-03T10:00:00+23:59").is_some());
        for bad in [
            &b""[..], b"2026-10-03 10:00:00Z", b"2026-10-03t10:00:00Z", b"2026-10-03T10:00:00z",
            b"2026-10-03T10:00:00", b"2026-10-03T10:00:00.000Z", b"1791071890000.0", b"1.79e12", b"-5",
            b"2026-13-03T10:00:00Z", b"2026-02-30T10:00:00Z", b"2027-02-29T10:00:00Z", b"2026-10-03T24:00:00Z",
            b"2026-10-03T10:00:60Z", b"2026-10-03T10:00:00+24:00", b"2026-10-03T10:00:00+05:60",
            b"2026-10-03T10:00:00+0200", b"999999999999999999999", b"1791071890000\r",
        ] {
            assert_eq!(parse(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
        assert!(parse(b"2028-02-29T10:00:00Z").is_some());
        assert_eq!(parse(b"00000000001791071890000"), Some(1791071890000));
        assert_eq!(parse(b"0000000000000000000000000000001791071890000"), Some(1791071890000));
        assert_eq!(parse(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse(b"9223372036854775808"), None);
        assert_eq!(parse(b"0000000000000000000000000000000000000000x"), None);
        assert_eq!(parse(b"2026-10-03T10:00:00+02:00x"), None);
    }

    #[test]
    fn calendar_exhaustive() {
        use super::{days_from_civil, days_in_month};
        for y in 1895i64..=2205 {
            for m in 1u32..=12 {
                for d in 1u32..=31 {
                    let text = format!("{y:04}-{m:02}-{d:02}T13:14:15Z");
                    let got = parse(text.as_bytes());
                    if d <= days_in_month(y, m) {
                        let want = (days_from_civil(y, m as i64, d as i64) * 86_400 + 13 * 3600 + 14 * 60 + 15) * 1000;
                        assert_eq!(got, Some(want), "{text}");
                    } else {
                        assert_eq!(got, None, "{text}");
                    }
                }
            }
        }
        assert_eq!(parse(b"1900-02-29T00:00:00Z"), None);
        assert!(parse(b"2000-02-29T00:00:00Z").is_some());
        assert_eq!(parse(b"2100-02-29T00:00:00Z"), None);
        assert_eq!(parse(b"1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse(b"0000-03-01T00:00:00Z"), Some(days_from_civil(0, 3, 1) * 86_400_000));
    }
}
