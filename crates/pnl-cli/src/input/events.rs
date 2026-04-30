use super::event_decode::{decode_event_line, EventDecodeConfig};
use anyhow::{Context, Result};
use pnl_core::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

pub struct EventIter<R> {
    lines: std::io::Lines<R>,
    base_currency: CurrencyId,
    money_scale: u8,
    line_number: usize,
    source: Option<String>,
}

fn open_events(
    path: &Path,
    base_currency: CurrencyId,
    money_scale: u8,
) -> Result<EventIter<BufReader<File>>> {
    let file = File::open(path).with_context(|| format!("reading {}", path.display()))?;
    let reader = BufReader::new(file);
    Ok(event_lines_with_source(
        reader,
        base_currency,
        money_scale,
        Some(path.display().to_string()),
    ))
}

pub fn open_replay_events(
    paths: &[PathBuf],
    base_currency: CurrencyId,
    money_scale: u8,
) -> Result<Vec<EventIter<BufReader<File>>>> {
    paths
        .iter()
        .map(|path| open_events(path, base_currency, money_scale))
        .collect()
}

#[cfg(test)]
pub(super) fn event_lines<R: BufRead>(
    reader: R,
    base_currency: CurrencyId,
    money_scale: u8,
) -> EventIter<R> {
    event_lines_with_source(reader, base_currency, money_scale, None)
}

fn event_lines_with_source<R: BufRead>(
    reader: R,
    base_currency: CurrencyId,
    money_scale: u8,
    source: Option<String>,
) -> EventIter<R> {
    EventIter {
        lines: reader.lines(),
        base_currency,
        money_scale,
        line_number: 0,
        source,
    }
}

impl<R: BufRead> Iterator for EventIter<R> {
    type Item = Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = self.lines.next()?;
            self.line_number += 1;
            let line_number = self.line_number;
            let line_context = event_line_context(line_number, self.source.as_deref());
            let line = match line.with_context(|| format!("reading {line_context}")) {
                Ok(line) => line,
                Err(error) => return Some(Err(error)),
            };
            if line.trim().is_empty() {
                continue;
            }
            return Some(decode_event_line(
                &line,
                EventDecodeConfig {
                    base_currency: self.base_currency,
                    money_scale: self.money_scale,
                },
                &line_context,
            ));
        }
    }
}

fn event_line_context(line_number: usize, source: Option<&str>) -> String {
    match source {
        Some(source) => format!("events line {line_number} ({source})"),
        None => format!("events line {line_number}"),
    }
}

#[cfg(test)]
pub(super) fn open_events_for_test(
    path: &Path,
    base_currency: CurrencyId,
    money_scale: u8,
) -> Result<EventIter<BufReader<File>>> {
    open_events(path, base_currency, money_scale)
}
