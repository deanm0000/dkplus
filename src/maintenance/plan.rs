//! Pure repack planning over catalog statistics; no I/O.
use super::rules::{Clock, Granularity, Interval, RepackLevel};
use chrono::{DateTime, Utc};
use std::collections::HashSet;

/// Catalog statistics for one current data file. Naive and date values are
/// represented as UTC instants with identical wall-clock fields.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileStat {
    pub(crate) path: String,
    pub(crate) min: DateTime<Utc>,
    /// Inclusive; for dates, midnight of the last day.
    pub(crate) max: DateTime<Utc>,
    pub(crate) dedup_min: Option<DateTime<Utc>>,
    pub(crate) dedup_max: Option<DateTime<Utc>>,
}

pub(crate) struct Context {
    pub(crate) clock: Clock,
    pub(crate) granularity: Granularity,
    /// Smallest step past an inclusive maximum: one day for dates, else 1µs.
    pub(crate) epsilon: Interval,
    pub(crate) now: DateTime<Utc>,
    pub(crate) has_dedup: bool,
    /// Largest time value in any current file.
    pub(crate) table_max: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Group {
    /// Earliest time value among the files.
    pub(crate) min: DateTime<Utc>,
    pub(crate) begin: DateTime<Utc>,
    pub(crate) end: DateTime<Utc>,
    pub(crate) files: Vec<String>,
}

impl Context {
    /// Exclusive end of the data a file covers.
    fn cover_end(&self, file: &FileStat) -> Option<DateTime<Utc>> {
        match self.granularity {
            Granularity::Fixed(step) => self.clock.add(file.max, step),
            Granularity::Unpredictable => self.clock.add(file.max, self.epsilon),
        }
    }

    fn span(&self, file: &FileStat, level: &RepackLevel) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let end = self.cover_end(file)?;
        match (self.granularity, level.calendar_begin) {
            (Granularity::Unpredictable, Some(unit)) => Some((
                self.clock.trunc(unit, file.min)?,
                self.clock.ceil(unit, end)?,
            )),
            _ => Some((file.min, end)),
        }
    }

    fn within(
        &self,
        begin: DateTime<Utc>,
        end: DateTime<Utc>,
        min: Option<Interval>,
        max: Option<Interval>,
    ) -> bool {
        let min_ok = min.is_none_or(|min| self.clock.add(begin, min).is_some_and(|t| t <= end));
        let max_ok = max.is_none_or(|max| self.clock.add(begin, max).is_some_and(|t| end <= t));
        min_ok && max_ok
    }

    /// Per-file rules: source window, dedup-timestamp age, and relative age.
    pub(crate) fn eligible(&self, file: &FileStat, level: &RepackLevel) -> bool {
        let Some((begin, end)) = self.span(file, level) else {
            return false;
        };
        if !self.within(begin, end, level.sourcewindow_min, level.sourcewindow_max) {
            return false;
        }
        let utc = Clock(None);
        let age_min = level.age_min.is_none_or(|age| {
            matches!((file.dedup_max, utc.sub(self.now, age)), (Some(newest), Some(cutoff)) if newest <= cutoff)
        });
        let age_max = level.age_max.is_none_or(|age| {
            matches!((file.dedup_min, utc.sub(self.now, age)), (Some(oldest), Some(cutoff)) if oldest >= cutoff)
        });
        let relage_min = level.relage_min.is_none_or(|relage| {
            matches!(
                (file.dedup_min, utc.add(file.max, relage)),
                (Some(oldest_dedup), Some(cutoff)) if oldest_dedup >= cutoff
            )
        });
        let relage_max = level.relage_max.is_none_or(|relage| {
            matches!(
                (file.dedup_max, utc.add(file.min, relage)),
                (Some(newest_dedup), Some(cutoff)) if newest_dedup <= cutoff
            )
        });
        age_min && age_max && relage_min && relage_max
    }
}

/// A growing run of files sorted by minimum time.
struct Run<'a> {
    context: &'a Context,
    level: &'a RepackLevel,
    min: DateTime<Utc>,
    max: DateTime<Utc>,
    cover: DateTime<Utc>,
    count: usize,
}

enum Extend {
    Continue,
    /// No longer prefix can satisfy the level.
    Stop,
}

impl<'a> Run<'a> {
    fn new(context: &'a Context, level: &'a RepackLevel, first: &FileStat) -> Option<Self> {
        Some(Self {
            context,
            level,
            min: first.min,
            max: first.max,
            cover: context.cover_end(first)?,
            count: 1,
        })
    }

    fn push(&mut self, file: &FileStat) -> Extend {
        let Some(cover) = self.context.cover_end(file) else {
            return Extend::Stop;
        };
        if matches!(self.context.granularity, Granularity::Fixed(_)) && file.min > self.cover {
            return Extend::Stop;
        }
        self.max = self.max.max(file.max);
        self.cover = self.cover.max(cover);
        self.count += 1;
        match (self.begin(), self.end(), self.level.targetwindow_max) {
            (Some(begin), Some(end), Some(max)) => {
                if self
                    .context
                    .clock
                    .add(begin, max)
                    .is_none_or(|limit| end > limit)
                {
                    Extend::Stop
                } else {
                    Extend::Continue
                }
            }
            (Some(_), Some(_), None) => Extend::Continue,
            _ => Extend::Stop,
        }
    }

    fn begin(&self) -> Option<DateTime<Utc>> {
        match (self.context.granularity, self.level.calendar_begin) {
            (Granularity::Unpredictable, Some(unit)) => self.context.clock.trunc(unit, self.min),
            _ => Some(self.min),
        }
    }

    fn end(&self) -> Option<DateTime<Utc>> {
        match (self.context.granularity, self.level.calendar_begin) {
            (Granularity::Unpredictable, Some(unit)) => self.context.clock.ceil(unit, self.cover),
            _ => Some(self.cover),
        }
    }

    /// The `[begin, end)` window if the current files satisfy the level.
    fn window(&self) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let (begin, end) = (self.begin()?, self.end()?);
        let clock = self.context.clock;
        let aligned = match (self.context.granularity, self.level.calendar_begin) {
            (Granularity::Fixed(_), Some(unit)) => {
                clock.is_aligned(unit, begin) && clock.is_aligned(unit, end)
            }
            _ => true,
        };
        let complete = match self.context.granularity {
            Granularity::Fixed(_) => true,
            Granularity::Unpredictable => self.context.table_max >= end,
        };
        (aligned
            && complete
            && self.context.within(
                begin,
                end,
                self.level.targetwindow_min,
                self.level.targetwindow_max,
            ))
        .then_some((begin, end))
    }
}

fn sort(files: &mut [&FileStat]) {
    files.sort_by(|a, b| (a.min, a.max, &a.path).cmp(&(b.min, b.max, &b.path)));
}

fn overlaps_others(
    all: &[FileStat],
    members: &HashSet<&str>,
    begin: DateTime<Utc>,
    end: DateTime<Utc>,
) -> bool {
    all.iter()
        .any(|file| !members.contains(file.path.as_str()) && file.min < end && file.max >= begin)
}

/// Greedy, disjoint groups for one level, in ascending time order. Each group
/// is the longest valid run starting at a file that no other current file overlaps.
pub(crate) fn plan_level(context: &Context, level: &RepackLevel, files: &[FileStat]) -> Vec<Group> {
    if level.uses_dedup_timestamp() && !context.has_dedup {
        return Vec::new();
    }
    let mut eligible: Vec<&FileStat> = files
        .iter()
        .filter(|file| context.eligible(file, level))
        .collect();
    sort(&mut eligible);
    let mut groups = Vec::new();
    let mut i = 0;
    while i < eligible.len() {
        let mut candidates = Vec::new();
        if let Some(mut run) = Run::new(context, level, eligible[i]) {
            for (j, file) in eligible.iter().enumerate().skip(i + 1) {
                let extend = run.push(file);
                if matches!(extend, Extend::Stop) && run.count != j - i + 1 {
                    break;
                }
                if let Some(window) = run.window() {
                    candidates.push((j, window));
                }
                if matches!(extend, Extend::Stop) {
                    break;
                }
            }
        }
        let found = candidates.into_iter().rev().find(|(j, (begin, end))| {
            let members: HashSet<&str> = eligible[i..=*j].iter().map(|f| f.path.as_str()).collect();
            !overlaps_others(files, &members, *begin, *end)
        });
        match found {
            Some((j, (begin, end))) => {
                groups.push(Group {
                    min: eligible[i].min,
                    begin,
                    end,
                    files: eligible[i..=j].iter().map(|f| f.path.clone()).collect(),
                });
                i = j + 1;
            }
            None => i += 1,
        }
    }
    groups
}

/// Re-evaluates an exact file set against a level; other overlapping files are
/// left to the repack's strictness.
pub(crate) fn evaluate(
    context: &Context,
    level: &RepackLevel,
    files: &[&FileStat],
) -> Result<(DateTime<Utc>, DateTime<Utc>), String> {
    if files.len() < 2 {
        return Err("fewer than two files remain".into());
    }
    if let Some(file) = files.iter().find(|file| !context.eligible(file, level)) {
        return Err(format!(
            "{} no longer satisfies the source window, age, or relage rules",
            file.path
        ));
    }
    let mut files = files.to_vec();
    sort(&mut files);
    let mut run = Run::new(context, level, files[0]).ok_or("file bounds are out of range")?;
    for file in &files[1..] {
        if matches!(run.push(file), Extend::Stop) && run.count != files.len() {
            return Err("files are not contiguous or exceed targetwindow_max".into());
        }
    }
    run.window().ok_or_else(|| {
        "files no longer satisfy the target window, calendar, or completeness rules".into()
    })
}

/// Orders by level then earliest time, dropping entries reusing any file of a
/// kept earlier entry.
pub(crate) fn dedupe<T>(
    mut entries: Vec<(u32, DateTime<Utc>, T)>,
    files: impl Fn(&T) -> &[String],
) -> Vec<T> {
    entries.sort_by_key(|(level, min, _)| (*level, *min));
    let mut claimed = HashSet::new();
    entries
        .into_iter()
        .filter(|(_, _, entry)| {
            let paths = files(entry);
            if paths.iter().any(|path| claimed.contains(path)) {
                return false;
            }
            claimed.extend(paths.iter().cloned());
            true
        })
        .map(|(_, _, entry)| entry)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::rules::CalendarUnit;
    use super::*;
    use chrono::{NaiveDate, TimeZone};

    const LA: chrono_tz::Tz = chrono_tz::America::Los_Angeles;

    fn la(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        LA.with_ymd_and_hms(y, m, d, h, min, 0)
            .earliest()
            .unwrap()
            .with_timezone(&Utc)
    }

    fn file(path: &str, min: DateTime<Utc>, max: DateTime<Utc>) -> FileStat {
        FileStat {
            path: path.into(),
            min,
            max,
            dedup_min: Some(min),
            dedup_max: Some(max),
        }
    }

    fn context(granularity: Granularity, table_max: DateTime<Utc>) -> Context {
        Context {
            clock: Clock(Some(LA)),
            granularity,
            epsilon: Interval::micros(1),
            now: la(2026, 6, 1, 0, 0),
            has_dedup: true,
            table_max,
        }
    }

    /// Hourly 5-minute files covering `[start, start + hours)`.
    fn hourly(day: u32, hours: std::ops::Range<u32>) -> Vec<FileStat> {
        hours
            .map(|h| {
                file(
                    &format!("d{day}h{h:02}"),
                    la(2026, 3, day, h, 0),
                    la(2026, 3, day, h, 55),
                )
            })
            .collect()
    }

    fn day_level() -> RepackLevel {
        RepackLevel {
            targetwindow_min: Some(Interval::days(1)),
            targetwindow_max: Some(Interval::days(1)),
            calendar_begin: Some(CalendarUnit::Day),
            ..RepackLevel::new(1)
        }
    }

    #[test]
    fn full_days_only_including_a_23_hour_dst_day() {
        // 2026-03-08 has 23 hours in Los Angeles; 2am does not exist.
        let mut files: Vec<_> = hourly(7, 0..24);
        files.extend((0..24).filter(|h| *h != 2).map(|h| {
            file(
                &format!("d8h{h:02}"),
                la(2026, 3, 8, h, 0),
                la(2026, 3, 8, h, 55),
            )
        }));
        files.extend(hourly(9, 0..12));
        let ctx = context(
            Granularity::Fixed(Interval::minutes(5)),
            la(2026, 3, 9, 11, 55),
        );
        let groups = plan_level(&ctx, &day_level(), &files);
        assert_eq!(groups.len(), 2);
        assert_eq!(
            (groups[0].begin, groups[0].end),
            (la(2026, 3, 7, 0, 0), la(2026, 3, 8, 0, 0))
        );
        assert_eq!(groups[0].files.len(), 24);
        assert_eq!(
            (groups[1].begin, groups[1].end),
            (la(2026, 3, 8, 0, 0), la(2026, 3, 9, 0, 0))
        );
        assert_eq!(groups[1].files.len(), 23);
    }

    #[test]
    fn gaps_and_foreign_overlaps_block_groups() {
        let mut files = hourly(10, 0..24);
        files.remove(5);
        let ctx = context(
            Granularity::Fixed(Interval::minutes(5)),
            la(2026, 3, 11, 0, 0),
        );
        assert!(plan_level(&ctx, &day_level(), &files).is_empty());

        let mut files = hourly(10, 0..24);
        // A small file overlapping the day is ineligible but still blocks it.
        files.push(file("late", la(2026, 3, 10, 3, 0), la(2026, 3, 10, 3, 0)));
        let level = RepackLevel {
            sourcewindow_min: Some(Interval::hours(1)),
            ..day_level()
        };
        assert!(plan_level(&ctx, &level, &files).is_empty());
    }

    #[test]
    fn ranges_take_the_longest_aligned_run_and_respect_age() {
        let days: Vec<_> = (1..=10)
            .map(|d| {
                file(
                    &format!("d{d}"),
                    la(2026, 1, d, 0, 0),
                    la(2026, 1, d, 23, 0),
                )
            })
            .collect();
        let level = RepackLevel {
            age_min: Some(Interval::days(140)),
            targetwindow_min: Some(Interval::days(1)),
            targetwindow_max: Some(Interval::days(7)),
            calendar_begin: Some(CalendarUnit::Day),
            ..RepackLevel::new(2)
        };
        let ctx = context(
            Granularity::Fixed(Interval::hours(1)),
            la(2026, 1, 10, 23, 0),
        );
        let groups = plan_level(&ctx, &level, &days);
        // now is 2026-06-01; 140 days earlier is 2026-01-12, so every file is old enough.
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].files.len(), 7);
        assert_eq!(groups[1].files, ["d8", "d9", "d10"]);
        let level = RepackLevel {
            age_min: Some(Interval::days(146)),
            ..level
        };
        // 146 days before 2026-06-01 is 2026-01-06T00:00 (PST), so d1-d5 qualify.
        assert_eq!(plan_level(&ctx, &level, &days)[0].files.len(), 5);
        let ctx = Context {
            has_dedup: true,
            ..ctx
        };
        let mut stale = days.clone();
        stale[0].dedup_max = None;
        assert_eq!(
            plan_level(&ctx, &level, &stale)[0].files,
            ["d2", "d3", "d4", "d5"]
        );
    }

    #[test]
    fn relative_age_uses_time_and_dedup_file_bounds() {
        let mut file = file("relative", la(2026, 3, 10, 1, 0), la(2026, 3, 10, 23, 0));
        file.dedup_min = Some(la(2026, 3, 17, 0, 0));
        file.dedup_max = Some(la(2026, 3, 18, 2, 0));
        let context = context(
            Granularity::Fixed(Interval::hours(1)),
            la(2026, 3, 11, 0, 0),
        );
        let level = RepackLevel {
            relage_min: Some(Interval::days(6)),
            relage_max: Some(Interval::days(9)),
            ..RepackLevel::new(1)
        };
        assert!(context.eligible(&file, &level));

        let level = RepackLevel {
            relage_min: Some(Interval::new(0, 6, 2 * 3_600_000_000)),
            ..level.clone()
        };
        assert!(!context.eligible(&file, &level));

        let level = RepackLevel {
            relage_min: Some(Interval::days(6)),
            relage_max: Some(Interval::days(8)),
            ..level
        };
        assert!(!context.eligible(&file, &level));
    }

    #[test]
    fn relative_age_accepts_negative_intervals() {
        let mut file = file("future", la(2026, 3, 10, 1, 0), la(2026, 3, 10, 23, 0));
        file.dedup_min = Some(la(2026, 3, 9, 0, 0));
        file.dedup_max = Some(la(2026, 3, 9, 12, 0));
        let context = context(
            Granularity::Fixed(Interval::hours(1)),
            la(2026, 3, 11, 0, 0),
        );
        let level = RepackLevel {
            relage_min: Some(Interval::days(-2)),
            relage_max: Some(Interval::hours(-12)),
            ..RepackLevel::new(1)
        };
        assert!(context.eligible(&file, &level));

        let level = RepackLevel {
            relage_min: Some(Interval::hours(-22)),
            ..level.clone()
        };
        assert!(!context.eligible(&file, &level));

        let level = RepackLevel {
            relage_min: Some(Interval::days(-2)),
            relage_max: Some(Interval::hours(-14)),
            ..level
        };
        assert!(!context.eligible(&file, &level));
    }

    #[test]
    fn relative_age_uses_elapsed_time_across_dst() {
        let mut file = file("dst", la(2026, 3, 9, 0, 0), la(2026, 3, 9, 0, 0));
        file.dedup_min = Some(la(2026, 3, 8, 0, 0));
        file.dedup_max = file.dedup_min;
        let context = context(
            Granularity::Fixed(Interval::hours(1)),
            la(2026, 3, 10, 0, 0),
        );
        let level = RepackLevel {
            relage_min: Some(Interval::hours(-23)),
            ..RepackLevel::new(1)
        };
        assert!(context.eligible(&file, &level));
        let level = RepackLevel {
            relage_min: Some(Interval::hours(-22)),
            ..level
        };
        assert!(!context.eligible(&file, &level));
    }

    #[test]
    fn months_from_day_files_with_unpredictable_granularity() {
        let mut files: Vec<_> = (1..=28)
            .map(|d| {
                file(
                    &format!("feb{d}"),
                    la(2026, 2, d, 0, 30),
                    la(2026, 2, d, 22, 10),
                )
            })
            .collect();
        // Sub-day sources are excluded by sourcewindow_min and break March.
        files.push(file("mar1", la(2026, 3, 1, 1, 0), la(2026, 3, 1, 1, 0)));
        let level = RepackLevel {
            sourcewindow_min: Some(Interval::days(1)),
            targetwindow_min: Some(Interval::months(1)),
            targetwindow_max: Some(Interval::months(1)),
            calendar_begin: Some(CalendarUnit::Month),
            ..RepackLevel::new(3)
        };
        let ctx = context(Granularity::Unpredictable, la(2026, 3, 1, 1, 0));
        let groups = plan_level(&ctx, &level, &files);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            (groups[0].begin, groups[0].end),
            (la(2026, 2, 1, 0, 0), la(2026, 3, 1, 0, 0))
        );
        assert_eq!(groups[0].files.len(), 28);
        // Completeness requires data at or after the month boundary.
        let ctx = context(Granularity::Unpredictable, la(2026, 2, 28, 23, 59));
        assert!(plan_level(&ctx, &level, &files).is_empty());
    }

    #[test]
    fn unpredictable_hours_complete_once_later_data_exists() {
        let files = vec![
            file("a", la(2026, 4, 1, 22, 0), la(2026, 4, 1, 22, 10)),
            file("b", la(2026, 4, 1, 22, 20), la(2026, 4, 1, 22, 30)),
            file("c", la(2026, 4, 1, 23, 30), la(2026, 4, 1, 23, 30)),
        ];
        let level = RepackLevel {
            targetwindow_min: Some(Interval::hours(1)),
            targetwindow_max: Some(Interval::hours(1)),
            calendar_begin: Some(CalendarUnit::Hour),
            ..RepackLevel::new(1)
        };
        let ctx = context(Granularity::Unpredictable, la(2026, 4, 1, 23, 30));
        let groups = plan_level(&ctx, &level, &files);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].files, ["a", "b"]);
        assert_eq!(groups[0].end, la(2026, 4, 1, 23, 0));
        // Without a calendar, completeness only needs a later table value.
        let level = RepackLevel {
            calendar_begin: None,
            targetwindow_min: None,
            targetwindow_max: Some(Interval::hours(1)),
            ..RepackLevel::new(1)
        };
        let groups = plan_level(&ctx, &level, &files);
        assert_eq!(groups[0].files, ["a", "b"]);
        assert_eq!(
            groups[0].end,
            la(2026, 4, 1, 22, 30) + chrono::Duration::microseconds(1)
        );
    }

    #[test]
    fn date_columns_use_inclusive_day_maxima() {
        let day = |d| {
            NaiveDate::from_ymd_opt(2026, 1, d)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
        };
        let files: Vec<_> = (1..=31)
            .map(|d| file(&format!("d{d}"), day(d), day(d)))
            .collect();
        let level = RepackLevel {
            targetwindow_min: Some(Interval::months(1)),
            targetwindow_max: Some(Interval::months(1)),
            calendar_begin: Some(CalendarUnit::Month),
            ..RepackLevel::new(1)
        };
        let ctx = Context {
            clock: Clock(None),
            epsilon: Interval::days(1),
            ..context(Granularity::Fixed(Interval::days(1)), day(31))
        };
        let groups = plan_level(&ctx, &level, &files);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].end.date_naive(),
            NaiveDate::from_ymd_opt(2026, 2, 1).unwrap()
        );
        let members: Vec<_> = files.iter().collect();
        assert!(evaluate(&ctx, &level, &members).is_ok());
        assert!(evaluate(&ctx, &level, &members[1..]).is_err());
    }

    #[test]
    fn earlier_levels_and_times_claim_files_first() {
        let entry = |files: &[&str]| files.iter().map(|f| f.to_string()).collect::<Vec<_>>();
        let t = |h| la(2026, 1, 1, h, 0);
        let kept = dedupe(
            vec![
                (2, t(0), entry(&["a", "b", "c"])),
                (1, t(5), entry(&["c", "d"])),
                (1, t(1), entry(&["a", "x"])),
                (3, t(0), entry(&["e"])),
            ],
            |files| files.as_slice(),
        );
        assert_eq!(
            kept,
            [entry(&["a", "x"]), entry(&["c", "d"]), entry(&["e"])]
        );
    }
}
