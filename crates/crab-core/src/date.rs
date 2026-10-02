// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Proleptic-Gregorian civil dates, as BigQuery's `DATE` type models them
//! (0001-01-01 ..= 9999-12-31), stored as days since 1970-01-01.
//!
//! The engine resolves relative phrases ("last quarter", "past 90 days") to
//! **literal** dates from a per-session clock, because a query containing
//! `CURRENT_DATE()` is excluded from BigQuery's free result cache. No time-zone
//! database is needed: the session clock is BigQuery's own `CURRENT_DATE(tz)`.
//!
//! Conversions use Howard Hinnant's `days_from_civil` / `civil_from_days`
//! algorithms (exact, branch-light integer arithmetic).

use crate::{Error, Result};
use std::fmt;

/// A calendar date. Ordered, hashable, 4 bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date {
    days: i32,
}

/// Days from 1970-01-01 to 0001-01-01.
const MIN_DAYS: i32 = -719_162;
/// Days from 1970-01-01 to 9999-12-31.
const MAX_DAYS: i32 = 2_932_896;

/// Calendar units for truncation and arithmetic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Grain {
    /// Calendar day.
    Day,
    /// ISO week starting Monday.
    IsoWeek,
    /// Week starting Sunday (BigQuery `WEEK`).
    Week,
    /// Calendar month.
    Month,
    /// Calendar quarter.
    Quarter,
    /// Calendar year.
    Year,
}

impl Date {
    /// Build from year, month (1-12) and day (1-31), validated.
    pub fn from_ymd(y: i32, m: u32, d: u32) -> Result<Self> {
        if !(1..=9999).contains(&y) || !(1..=12).contains(&m) || d == 0 || d > days_in_month(y, m) {
            return Err(Error::Date("out of range"));
        }
        Ok(Self { days: days_from_civil(y, m, d) })
    }

    /// Build from days since 1970-01-01, validated against BigQuery's range.
    pub fn from_days(days: i32) -> Result<Self> {
        if (MIN_DAYS..=MAX_DAYS).contains(&days) {
            Ok(Self { days })
        } else {
            Err(Error::Date("out of range"))
        }
    }

    /// Days since 1970-01-01.
    #[must_use]
    pub fn days(self) -> i32 {
        self.days
    }

    /// `(year, month, day)`.
    #[must_use]
    pub fn ymd(self) -> (i32, u32, u32) {
        civil_from_days(self.days)
    }

    /// ISO weekday, Monday = 1 .. Sunday = 7.
    #[must_use]
    pub fn iso_weekday(self) -> u32 {
        // 1970-01-01 was a Thursday (4).
        ((self.days.rem_euclid(7) + 3) % 7 + 1) as u32
    }

    /// Parse a canonical `YYYY-MM-DD` literal.
    pub fn parse(s: &str) -> Result<Self> {
        let b = s.as_bytes();
        if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
            return Err(Error::Date("expected YYYY-MM-DD"));
        }
        let num = |r: std::ops::Range<usize>| -> Result<u32> {
            b[r].iter().try_fold(0u32, |acc, &c| {
                if c.is_ascii_digit() {
                    Ok(acc * 10 + u32::from(c - b'0'))
                } else {
                    Err(Error::Date("expected digits"))
                }
            })
        };
        Self::from_ymd(num(0..4)? as i32, num(5..7)?, num(8..10)?)
    }

    /// Add (or subtract) whole days.
    pub fn add_days(self, n: i32) -> Result<Self> {
        Self::from_days(self.days.checked_add(n).ok_or(Error::Date("overflow"))?)
    }

    /// Add (or subtract) calendar months, clamping the day to the target
    /// month's length (BigQuery `DATE_ADD(..., INTERVAL n MONTH)` semantics).
    pub fn add_months(self, n: i32) -> Result<Self> {
        let (y, m, d) = self.ymd();
        let total = y.checked_mul(12).and_then(|v| v.checked_add(m as i32 - 1 + n)).ok_or(Error::Date("overflow"))?;
        let (ny, nm) = (total.div_euclid(12), (total.rem_euclid(12) + 1) as u32);
        if !(1..=9999).contains(&ny) {
            return Err(Error::Date("out of range"));
        }
        Self::from_ymd(ny, nm, d.min(days_in_month(ny, nm)))
    }

    /// First day of the period containing this date (`DATE_TRUNC`).
    #[must_use]
    pub fn trunc(self, grain: Grain) -> Self {
        let (y, m, _) = self.ymd();
        let days = match grain {
            Grain::Day => self.days,
            Grain::IsoWeek => self.days - (self.iso_weekday() as i32 - 1),
            Grain::Week => self.days - (self.iso_weekday() % 7) as i32,
            Grain::Month => days_from_civil(y, m, 1),
            Grain::Quarter => days_from_civil(y, (m - 1) / 3 * 3 + 1, 1),
            Grain::Year => days_from_civil(y, 1, 1),
        };
        // Truncation never leaves the range except before 0001-01-01's week.
        Self { days: days.max(MIN_DAYS) }
    }

    /// Add `n` periods of `grain`.
    pub fn add(self, grain: Grain, n: i32) -> Result<Self> {
        match grain {
            Grain::Day => self.add_days(n),
            Grain::IsoWeek | Grain::Week => self.add_days(n.checked_mul(7).ok_or(Error::Date("overflow"))?),
            Grain::Month => self.add_months(n),
            Grain::Quarter => self.add_months(n.checked_mul(3).ok_or(Error::Date("overflow"))?),
            Grain::Year => self.add_months(n.checked_mul(12).ok_or(Error::Date("overflow"))?),
        }
    }

    /// The half-open range `[start, end)` of the *previous full* period
    /// relative to `self` ("last quarter", "last month"), as literals.
    pub fn previous_period(self, grain: Grain) -> Result<(Self, Self)> {
        let end = self.trunc(grain);
        Ok((end.add(grain, -1)?, end))
    }
}

/// Microseconds in a day.
pub const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Parse `HH:MM:SS[.ffffff]` (BigQuery `TIME`) to microseconds since midnight.
/// Fractions shorter than 6 digits are scaled; longer ones are rejected.
pub fn parse_time(s: &str) -> Result<i64> {
    let b = s.as_bytes();
    if b.len() < 8 || b[2] != b':' || b[5] != b':' {
        return Err(Error::Date("expected HH:MM:SS"));
    }
    let two = |at: usize| -> Result<i64> {
        match (b[at], b[at + 1]) {
            (x @ b'0'..=b'9', y @ b'0'..=b'9') => Ok(i64::from(x - b'0') * 10 + i64::from(y - b'0')),
            _ => Err(Error::Date("expected digits")),
        }
    };
    let (h, m, sec) = (two(0)?, two(3)?, two(6)?);
    if h > 23 || m > 59 || sec > 59 {
        return Err(Error::Date("time out of range"));
    }
    let mut micros = 0i64;
    if b.len() > 8 {
        let frac = &b[8..];
        if frac[0] != b'.' || frac.len() < 2 || frac.len() > 7 || !frac[1..].iter().all(u8::is_ascii_digit) {
            return Err(Error::Date("bad fraction"));
        }
        for (i, &d) in frac[1..].iter().enumerate() {
            micros += i64::from(d - b'0') * 10i64.pow(5 - i as u32);
        }
    }
    Ok(((h * 60 + m) * 60 + sec) * 1_000_000 + micros)
}

/// Parse `YYYY-MM-DD[T| ]HH:MM:SS[.ffffff]` (BigQuery `DATETIME`) to
/// microseconds since 1970-01-01T00:00:00 (civil, no zone).
pub fn parse_datetime(s: &str) -> Result<i64> {
    if s.len() < 19 || !matches!(s.as_bytes()[10], b'T' | b' ') {
        return Err(Error::Date("expected YYYY-MM-DDTHH:MM:SS"));
    }
    let d = Date::parse(&s[..10])?;
    Ok(i64::from(d.days()) * MICROS_PER_DAY + parse_time(&s[11..])?)
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (y, m, d) = self.ymd();
        write!(f, "{y:04}-{m:02}-{d:02}")
    }
}

/// True for Gregorian leap years.
#[must_use]
pub fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Days in month `m` (1-12) of year `y`.
#[must_use]
pub fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => 0,
    }
}

fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400) as u32;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i32 - 719_468
}

fn civil_from_days(z: i32) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe as i32 + era * 400 + i32::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Date {
        Date::parse(s).unwrap()
    }

    #[test]
    fn epoch_and_bounds() {
        assert_eq!(d("1970-01-01").days(), 0);
        assert_eq!(d("0001-01-01").days(), MIN_DAYS);
        assert_eq!(d("9999-12-31").days(), MAX_DAYS);
        assert!(Date::from_days(MAX_DAYS + 1).is_err());
    }

    #[test]
    fn round_trip_across_range() {
        for days in (MIN_DAYS..=MAX_DAYS).step_by(997) {
            let dt = Date::from_days(days).unwrap();
            assert_eq!(Date::parse(&dt.to_string()).unwrap(), dt);
        }
    }

    #[test]
    fn rejects_invalid_literals() {
        for bad in ["2026-02-30", "2026-13-01", "2026-1-01", "20260101", "abcd-ef-gh", "0000-01-01"] {
            assert!(Date::parse(bad).is_err(), "{bad}");
        }
        assert!(Date::parse("2024-02-29").is_ok());
        assert!(Date::parse("2023-02-29").is_err());
    }

    #[test]
    fn weekday() {
        assert_eq!(d("1970-01-01").iso_weekday(), 4);
        assert_eq!(d("2026-09-28").iso_weekday(), 1);
        assert_eq!(d("2026-09-27").iso_weekday(), 7);
    }

    #[test]
    fn trunc_matches_bigquery() {
        let x = d("2026-08-15"); // Saturday
        assert_eq!(x.trunc(Grain::Month), d("2026-08-01"));
        assert_eq!(x.trunc(Grain::Quarter), d("2026-07-01"));
        assert_eq!(x.trunc(Grain::Year), d("2026-01-01"));
        assert_eq!(x.trunc(Grain::IsoWeek), d("2026-08-10"));
        assert_eq!(x.trunc(Grain::Week), d("2026-08-09"));
        assert_eq!(d("2026-08-09").trunc(Grain::Week), d("2026-08-09"), "Sunday is its own WEEK start");
    }

    #[test]
    fn add_months_clamps() {
        assert_eq!(d("2026-01-31").add_months(1).unwrap(), d("2026-02-28"));
        assert_eq!(d("2024-01-31").add_months(1).unwrap(), d("2024-02-29"));
        assert_eq!(d("2026-03-15").add_months(-15).unwrap(), d("2024-12-15"));
        assert!(d("9999-12-01").add_months(1).is_err());
    }

    #[test]
    fn times_and_datetimes() {
        assert_eq!(parse_time("00:00:00").unwrap(), 0);
        assert_eq!(parse_time("23:59:59.999999").unwrap(), MICROS_PER_DAY - 1);
        assert_eq!(parse_time("01:02:03.5").unwrap(), 3_723_500_000);
        for bad in ["24:00:00", "1:02:03", "01:02:03.", "01:02:03.1234567", "01:60:00"] {
            assert!(parse_time(bad).is_err(), "{bad}");
        }
        assert_eq!(parse_datetime("1970-01-02T00:00:01").unwrap(), MICROS_PER_DAY + 1_000_000);
        assert_eq!(parse_datetime("1969-12-31 23:59:59").unwrap(), -1_000_000);
        assert!(parse_datetime("2026-01-01X00:00:00").is_err());
    }

    #[test]
    fn previous_period_is_half_open() {
        let today = d("2026-09-29");
        assert_eq!(today.previous_period(Grain::Quarter).unwrap(), (d("2026-04-01"), d("2026-07-01")));
        assert_eq!(today.previous_period(Grain::Month).unwrap(), (d("2026-08-01"), d("2026-09-01")));
        assert_eq!(d("2026-01-10").previous_period(Grain::Year).unwrap(), (d("2025-01-01"), d("2026-01-01")));
    }
}
