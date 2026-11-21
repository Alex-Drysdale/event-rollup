//! Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, F3 epoch
//! milliseconds as ASCII digits. Returns milliseconds since the epoch, UTC, or
//! None for anything else (including an F3 value too big for an i64, which can
//! only be outside every window anyway).

pub const DAY_MS: i64 = 86_400_000;

#[inline]
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
#[inline]
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

#[inline]
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

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// The first 19 bytes, `YYYY-MM-DDTHH:MM:SS`, as epoch milliseconds.
#[inline]
fn datetime(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = (d2(b, 0)? * 100 + d2(b, 2)?) as i64;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let days = days_from_civil(y, mo as i64, d as i64);
    Some((days * 86_400 + (h * 3600 + mi * 60 + s) as i64) * 1000)
}

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
}
