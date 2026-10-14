// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Evaluation items and loaders.
//!
//! Loads the BIRD-family JSON layout used by the corrected sets
//! (Arcwise-Plat-SQL / -Full, BIRD-Platinum) — an array of objects with
//! `question_id`, `db_id`, `question`, `evidence`, `SQL`, `difficulty` — and
//! the same fields as JSON lines. Unknown fields are ignored. An item whose
//! gold is absent or empty is **unanswerable**: the correct behaviour there is
//! to clarify, abstain or refuse, and the scorecard counts it that way.

use crate::base::json::{Event, Scanner};
use crate::base::{Error, Result};

/// One evaluation question.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item {
    /// Stable id (string form of `question_id`, or the line number).
    pub id: String,
    /// Database the question is asked against.
    pub db_id: String,
    /// The question.
    pub question: String,
    /// External-knowledge hint (may be empty).
    pub evidence: String,
    /// Verified gold SQL; `None` when the question is unanswerable.
    pub gold_sql: Option<String>,
    /// Difficulty label (`simple` / `moderate` / `challenging`), if given.
    pub difficulty: Option<String>,
}

impl Item {
    /// True when a correct SQL answer exists.
    #[must_use]
    pub fn answerable(&self) -> bool {
        self.gold_sql.is_some()
    }
}

/// A named list of items.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Dataset {
    /// Name used in reports.
    pub name: String,
    /// Items, in file order.
    pub items: Vec<Item>,
}

impl Dataset {
    /// Parse a JSON array of BIRD-style objects.
    pub fn from_json_array(name: &str, src: &str) -> Result<Self> {
        let mut s = Scanner::new(src);
        if s.expect()? != Event::ArrayStart {
            return Err(Error::Json { at: 0, msg: "dataset must be a JSON array" });
        }
        let mut items = Vec::new();
        let mut buf = String::new();
        loop {
            match s.expect()? {
                Event::ArrayEnd => break,
                Event::ObjectStart => items.push(parse_item(&mut s, &mut buf, items.len())?),
                _ => {
                    return Err(Error::Json {
                        at: s.offset(),
                        msg: "dataset entries must be objects",
                    })
                }
            }
        }
        if s.next_event()?.is_some() {
            return Err(Error::Json { at: s.offset(), msg: "trailing data" });
        }
        Ok(Self { name: name.to_owned(), items })
    }

    /// Parse JSON lines of BIRD-style objects.
    pub fn from_jsonl(name: &str, src: &str) -> Result<Self> {
        let mut items = Vec::new();
        let mut buf = String::new();
        for line in src.lines().filter(|l| !l.trim().is_empty()) {
            let mut s = Scanner::new(line);
            if s.expect()? != Event::ObjectStart {
                return Err(Error::Json { at: 0, msg: "dataset lines must be objects" });
            }
            items.push(parse_item(&mut s, &mut buf, items.len())?);
        }
        Ok(Self { name: name.to_owned(), items })
    }

    /// Items belonging to one database, in order.
    pub fn for_db<'a>(&'a self, db_id: &'a str) -> impl Iterator<Item = &'a Item> + 'a {
        self.items.iter().filter(move |i| i.db_id == db_id)
    }
}

fn parse_item(s: &mut Scanner<'_>, buf: &mut String, ordinal: usize) -> Result<Item> {
    let mut it = Item {
        id: String::new(),
        db_id: String::new(),
        question: String::new(),
        evidence: String::new(),
        gold_sql: None,
        difficulty: None,
    };
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) => {
                let name = k.as_plain().unwrap_or("");
                let v = s.expect()?;
                match (name, v) {
                    ("question_id" | "id", Event::Num(n)) => n.clone_into(&mut it.id),
                    ("question_id" | "id", Event::Str(x)) => x.decode(buf)?.clone_into(&mut it.id),
                    ("db_id", Event::Str(x)) => x.decode(buf)?.clone_into(&mut it.db_id),
                    ("question", Event::Str(x)) => x.decode(buf)?.clone_into(&mut it.question),
                    ("evidence", Event::Str(x)) => x.decode(buf)?.clone_into(&mut it.evidence),
                    ("SQL" | "sql" | "gold_sql", Event::Str(x)) => {
                        let sql = x.decode(buf)?.trim();
                        it.gold_sql = (!sql.is_empty()).then(|| sql.to_owned());
                    }
                    ("difficulty", Event::Str(x)) => {
                        it.difficulty = Some(x.decode(buf)?.to_owned());
                    }
                    (_, other) => s.skip(other)?,
                }
            }
            _ => return Err(Error::Json { at: s.offset(), msg: "expected key" }),
        }
    }
    if it.id.is_empty() {
        it.id = ordinal.to_string();
    }
    if it.question.is_empty() || it.db_id.is_empty() {
        return Err(Error::Json { at: s.offset(), msg: "item needs question and db_id" });
    }
    Ok(it)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BIRD: &str = r#"[
      {"question_id": 0, "db_id": "california_schools", "question": "What is the highest eligible free rate?",
       "evidence": "Eligible free rate = `Free Meal Count` / `Enrollment`", "SQL": "SELECT MAX(x) FROM frpm", "difficulty": "simple",
       "extra": {"ignored": [1, 2]}},
      {"question_id": "q1", "db_id": "shop", "question": "Which color is best?", "SQL": "  "}
    ]"#;

    #[test]
    fn loads_bird_layout_and_marks_unanswerable() {
        let d = Dataset::from_json_array("mini", BIRD).unwrap();
        assert_eq!(d.items.len(), 2);
        assert_eq!(d.items[0].id, "0");
        assert!(d.items[0].evidence.contains("Free Meal Count"));
        assert_eq!(d.items[0].difficulty.as_deref(), Some("simple"));
        assert!(d.items[0].answerable());
        assert!(!d.items[1].answerable(), "blank gold = unanswerable");
        assert_eq!(d.for_db("shop").count(), 1);
    }

    #[test]
    fn loads_jsonl() {
        let src = "{\"db_id\":\"a\",\"question\":\"q1\",\"SQL\":\"SELECT 1\"}\n\n{\"db_id\":\"a\",\"question\":\"q2\"}\n";
        let d = Dataset::from_jsonl("l", src).unwrap();
        assert_eq!(d.items.len(), 2);
        assert_eq!(d.items[1].id, "1", "ordinal id when none is given");
    }

    #[test]
    fn rejects_bad_files() {
        assert!(Dataset::from_json_array("x", "{}").is_err());
        assert!(Dataset::from_json_array("x", "[1]").is_err());
        assert!(Dataset::from_json_array("x", r#"[{"db_id":"a"}]"#).is_err(), "question required");
        assert!(Dataset::from_json_array("x", "[] []").is_err());
    }
}
