//! Repack-level rules and the wall-clock arithmetic used to evaluate them.
use crate::Result;
use crate::error::fail;
use chrono::{
    DateTime, Datelike, Duration, Months, NaiveDate, NaiveDateTime, Offset, TimeZone, Utc,
};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

/// A Postgres-style interval. As with `timestamptz + interval`, months and days
/// are applied to local wall-clock time and microseconds to elapsed time, so
/// `1 day` spans 23- and 25-hour days and `1 month` spans the calendar month.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Interval {
    pub const fn new(months: i32, days: i32, micros: i64) -> Self {
        Self {
            months,
            days,
            micros,
        }
    }

    pub const fn months(months: i32) -> Self {
        Self::new(months, 0, 0)
    }

    pub const fn days(days: i32) -> Self {
        Self::new(0, days, 0)
    }

    pub const fn hours(hours: i64) -> Self {
        Self::new(0, 0, hours * 3_600_000_000)
    }

    pub const fn minutes(minutes: i64) -> Self {
        Self::new(0, 0, minutes * 60_000_000)
    }

    pub const fn micros(micros: i64) -> Self {
        Self::new(0, 0, micros)
    }

    pub fn is_zero(self) -> bool {
        self == Self::default()
    }

    pub(crate) fn is_non_negative(self) -> bool {
        self.months >= 0 && self.days >= 0 && self.micros >= 0
    }

    fn negate(self) -> Self {
        Self::new(-self.months, -self.days, -self.micros)
    }
}

impl std::fmt::Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} mons {} days {} microseconds",
            self.months, self.days, self.micros
        )
    }
}

/// Spacing of `time_column` values. `Unpredictable` infers completeness from
/// later data in the table instead of from a fixed step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    Fixed(Interval),
    Unpredictable,
}

/// Calendar or clock unit aligning both ends of a repack target window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CalendarUnit {
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl std::str::FromStr for CalendarUnit {
    type Err = crate::Error;

    fn from_str(value: &str) -> Result<Self> {
        Ok(match value {
            "minute" => Self::Minute,
            "hour" => Self::Hour,
            "day" => Self::Day,
            "week" => Self::Week,
            "month" => Self::Month,
            "quarter" => Self::Quarter,
            "year" => Self::Year,
            _ => fail!(
                InvalidInput,
                "calendar unit {value:?} must be one of minute, hour, day, week, month, quarter, year"
            ),
        })
    }
}

/// One maintenance repack level. Every configured rule must hold (bounds are
/// inclusive); unset rules are unconstrained.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepackLevel {
    pub level: u32,
    /// Size bounds of the repacked output, from its first value to its exclusive end.
    pub targetwindow_min: Option<Interval>,
    pub targetwindow_max: Option<Interval>,
    /// Size bounds of each input file.
    pub sourcewindow_min: Option<Interval>,
    pub sourcewindow_max: Option<Interval>,
    /// Minimum age of each input file's newest `dedup_timestamp` value.
    pub age_min: Option<Interval>,
    /// Maximum age of each input file's oldest `dedup_timestamp` value.
    pub age_max: Option<Interval>,
    /// Minimum `dedup_timestamp - time_column` for each input file.
    pub relage_min: Option<Interval>,
    /// Maximum `dedup_timestamp - time_column` for each input file.
    pub relage_max: Option<Interval>,
    pub calendar_begin: Option<CalendarUnit>,
}

impl RepackLevel {
    pub fn new(level: u32) -> Self {
        Self {
            level,
            ..Self::default()
        }
    }

    pub(crate) fn uses_dedup_timestamp(&self) -> bool {
        self.age_min.is_some()
            || self.age_max.is_some()
            || self.relage_min.is_some()
            || self.relage_max.is_some()
    }

    pub(crate) fn intervals(&self) -> impl Iterator<Item = (&'static str, Interval)> + '_ {
        [
            ("targetwindow_min", self.targetwindow_min),
            ("targetwindow_max", self.targetwindow_max),
            ("sourcewindow_min", self.sourcewindow_min),
            ("sourcewindow_max", self.sourcewindow_max),
            ("age_min", self.age_min),
            ("age_max", self.age_max),
            ("relage_min", self.relage_min),
            ("relage_max", self.relage_max),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.map(|value| (name, value)))
    }
}

/// Instant arithmetic in a table's wall clock. `None` means naive/date values,
/// stored as UTC instants with identical wall-clock fields.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Clock(pub(crate) Option<Tz>);

impl Clock {
    pub(crate) fn local(self, time: DateTime<Utc>) -> NaiveDateTime {
        match self.0 {
            Some(tz) => time.with_timezone(&tz).naive_local(),
            None => time.naive_utc(),
        }
    }

    fn offset(self, time: DateTime<Utc>) -> Duration {
        match self.0 {
            Some(tz) => Duration::seconds(i64::from(
                tz.offset_from_utc_datetime(&time.naive_utc())
                    .fix()
                    .local_minus_utc(),
            )),
            None => Duration::zero(),
        }
    }

    /// Earliest instant with this wall-clock time; times skipped by a DST gap
    /// resolve to the end of the gap.
    pub(crate) fn resolve(self, local: NaiveDateTime) -> Option<DateTime<Utc>> {
        let Some(tz) = self.0 else {
            return Some(local.and_utc());
        };
        let mut candidate = local;
        for _ in 0..=48 * 60 {
            if let Some(time) = tz.from_local_datetime(&candidate).earliest() {
                return Some(time.with_timezone(&Utc));
            }
            candidate = candidate.checked_add_signed(Duration::minutes(1))?;
        }
        None
    }

    pub(crate) fn add(self, time: DateTime<Utc>, interval: Interval) -> Option<DateTime<Utc>> {
        let mut time = time;
        if interval.months != 0 || interval.days != 0 {
            let mut local = self.local(time);
            local = if interval.months >= 0 {
                local.checked_add_months(Months::new(interval.months as u32))?
            } else {
                local.checked_sub_months(Months::new(interval.months.unsigned_abs()))?
            };
            local = local.checked_add_signed(Duration::days(i64::from(interval.days)))?;
            time = self.resolve(local)?;
        }
        time.checked_add_signed(Duration::microseconds(interval.micros))
    }

    pub(crate) fn sub(self, time: DateTime<Utc>, interval: Interval) -> Option<DateTime<Utc>> {
        self.add(time, interval.negate())
    }

    pub(crate) fn trunc(self, unit: CalendarUnit, time: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Clock units follow the instant's own offset so repeated or skipped DST
        // hours still have distinct, elapsed-time boundaries.
        let clock = |step: i64| {
            let offset = self.offset(time);
            let local = time.naive_utc().checked_add_signed(offset)?;
            let seconds = local.and_utc().timestamp().div_euclid(step) * step;
            let local = DateTime::from_timestamp(seconds, 0)?.naive_utc();
            Some(local.checked_sub_signed(offset)?.and_utc())
        };
        let date = |date: NaiveDate| self.resolve(date.and_hms_opt(0, 0, 0)?);
        let local = self.local(time).date();
        match unit {
            CalendarUnit::Minute => clock(60),
            CalendarUnit::Hour => clock(3_600),
            CalendarUnit::Day => date(local),
            CalendarUnit::Week => date(local.checked_sub_signed(Duration::days(i64::from(
                local.weekday().num_days_from_monday(),
            )))?),
            CalendarUnit::Month => date(local.with_day(1)?),
            CalendarUnit::Quarter => date(NaiveDate::from_ymd_opt(
                local.year(),
                (local.month0() / 3) * 3 + 1,
                1,
            )?),
            CalendarUnit::Year => date(NaiveDate::from_ymd_opt(local.year(), 1, 1)?),
        }
    }

    /// The first unit boundary at or after `time`.
    pub(crate) fn ceil(self, unit: CalendarUnit, time: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let floor = self.trunc(unit, time)?;
        if floor == time {
            return Some(time);
        }
        match unit {
            CalendarUnit::Minute => floor.checked_add_signed(Duration::minutes(1)),
            CalendarUnit::Hour => floor.checked_add_signed(Duration::hours(1)),
            CalendarUnit::Day => self.add(floor, Interval::days(1)),
            CalendarUnit::Week => self.add(floor, Interval::days(7)),
            CalendarUnit::Month => self.add(floor, Interval::months(1)),
            CalendarUnit::Quarter => self.add(floor, Interval::months(3)),
            CalendarUnit::Year => self.add(floor, Interval::months(12)),
        }
    }

    pub(crate) fn is_aligned(self, unit: CalendarUnit, time: DateTime<Utc>) -> bool {
        self.trunc(unit, time) == Some(time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn day_and_month_intervals_follow_the_wall_clock() {
        let la = Clock(Some(chrono_tz::America::Los_Angeles));
        let spring = utc("2026-03-08T00:00:00-08:00");
        let next = la.add(spring, Interval::days(1)).unwrap();
        assert_eq!(next, utc("2026-03-09T00:00:00-07:00"));
        assert_eq!((next - spring).num_hours(), 23);
        assert_eq!(
            la.add(spring, Interval::hours(24)).unwrap(),
            utc("2026-03-09T01:00:00-07:00")
        );
        let feb = utc("2026-02-01T00:00:00-08:00");
        assert_eq!(
            la.add(feb, Interval::months(1)).unwrap(),
            utc("2026-03-01T00:00:00-08:00")
        );
        let naive = Clock(None);
        let jan31 = NaiveDate::from_ymd_opt(2026, 1, 31)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(
            naive.add(jan31, Interval::months(1)).unwrap().date_naive(),
            NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()
        );
        assert_eq!(naive.sub(jan31, Interval::days(31)).unwrap().day(), 31);
    }

    #[test]
    fn calendar_units_align_in_local_time() {
        let ny = Clock(Some(chrono_tz::America::New_York));
        let t = utc("2026-05-14T15:42:10-04:00");
        assert_eq!(
            ny.trunc(CalendarUnit::Hour, t).unwrap(),
            utc("2026-05-14T15:00:00-04:00")
        );
        assert_eq!(
            ny.trunc(CalendarUnit::Day, t).unwrap(),
            utc("2026-05-14T00:00:00-04:00")
        );
        assert_eq!(
            ny.trunc(CalendarUnit::Week, t).unwrap(),
            utc("2026-05-11T00:00:00-04:00")
        );
        assert_eq!(
            ny.trunc(CalendarUnit::Quarter, t).unwrap(),
            utc("2026-04-01T00:00:00-04:00")
        );
        assert_eq!(
            ny.ceil(CalendarUnit::Month, t).unwrap(),
            utc("2026-06-01T00:00:00-04:00")
        );
        assert_eq!(
            ny.ceil(CalendarUnit::Year, t).unwrap(),
            utc("2027-01-01T00:00:00-05:00")
        );
        let midnight = utc("2026-05-14T00:00:00-04:00");
        assert!(ny.is_aligned(CalendarUnit::Day, midnight));
        assert!(!ny.is_aligned(CalendarUnit::Month, midnight));
    }

    #[test]
    fn clock_units_keep_repeated_dst_hours_distinct() {
        let ny = Clock(Some(chrono_tz::America::New_York));
        let first = utc("2026-11-01T01:30:00-04:00");
        let second = utc("2026-11-01T01:30:00-05:00");
        assert_eq!(
            ny.ceil(CalendarUnit::Hour, first).unwrap(),
            utc("2026-11-01T01:00:00-05:00")
        );
        assert_eq!(
            ny.trunc(CalendarUnit::Hour, second).unwrap(),
            utc("2026-11-01T01:00:00-05:00")
        );
        let india = Clock(Some(chrono_tz::Asia::Kolkata));
        assert_eq!(
            india
                .trunc(CalendarUnit::Hour, utc("2026-01-01T10:20:00+05:30"))
                .unwrap(),
            utc("2026-01-01T10:00:00+05:30")
        );
    }

    #[test]
    fn skipped_local_midnights_resolve_to_the_gap_end() {
        // Santiago skips local midnight when DST starts.
        let santiago = Clock(Some(chrono_tz::America::Santiago));
        let t = utc("2026-09-06T12:00:00-03:00");
        assert_eq!(
            santiago.trunc(CalendarUnit::Day, t).unwrap(),
            utc("2026-09-06T01:00:00-03:00")
        );
    }

    #[test]
    fn calendar_units_parse_from_tags() {
        assert_eq!(
            "month".parse::<CalendarUnit>().unwrap(),
            CalendarUnit::Month
        );
        assert!("1 hour".parse::<CalendarUnit>().is_err());
    }
}
