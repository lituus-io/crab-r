// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! What goes in the prompt for one question.
//!
//! - **Small schemas** (fit [`EngineConfig::schema_token_budget`]): the
//!   whole pre-rendered schema, byte-identical for every question (the
//!   provider's prefix cache serves it at the cached-input price).
//! - **Large schemas**: the linked tables only. Linking is lexical first
//!   (`link` BM25 over table cards, boosted by value grounding) — free
//!   and sub-millisecond. When the model accepts a JSON schema, one small
//!   call then picks the tables from compact cards of the candidates under
//!   an **enum of real table names** (it cannot name a table that does not
//!   exist). Bridge tables on the join paths are added, and the joins
//!   themselves are listed after the tables.
//!
//! Grounded values and the schema grammar are restricted to the tables in
//! context.
//!
//! [`EngineConfig::schema_token_budget`]: crate::engine::EngineConfig::schema_token_budget

use crate::base::json::{Event, Scanner, Writer};
use crate::base::Budget;
use crate::catalog::TableId;
use crate::engine::{AnswerCache, Dialect, Error, Events, Result, Session, Stage};
use crate::exec::Warehouse;
use crate::link::LinkOpts;
use crate::llm::{Constraint, GenRequest, Model, Part, Prompt};
use crate::prompt::{literal_into, table_card, LINK_SYSTEM};
use crate::values::MatchKind;
use std::fmt::Write as _;

/// Boost of a table holding a value the question names (a fraction of the
/// best word score): exact matches are as strong as the best word match.
const BOOST_EXACT: f32 = 1.0;
const BOOST_FUZZY: f32 = 0.6;

impl<M: Model, W: Warehouse, C: AnswerCache> Session<'_, M, W, C> {
    /// Choose the tables for `question` and render them into
    /// `schema_buf`. Returns (tables, approximate tokens).
    pub(crate) async fn context<E: Events>(
        &mut self,
        question: &str,
        events: &mut E,
        budget: &mut Budget,
    ) -> Result<(u32, u32)> {
        let e = self.e;
        self.grounded.clear();
        if e.cfg.max_grounded > 0 && !e.values.is_empty() {
            self.grounded = e.values.ground(question, e.cfg.max_grounded * 2);
        }
        let s = &e.schema;
        let budget_bytes = graph_r::text::bytes_for_tokens(u64::from(e.cfg.schema_token_budget));
        self.schema_buf.clear();
        self.ctx.clear();
        if s.text().len() <= budget_bytes {
            self.schema_buf.push_str(s.text());
            self.ctx.extend(s.blocks().iter().map(|b| b.table));
            return Ok((self.ctx.len() as u32, s.estimated_tokens()));
        }
        let chosen = self.link(question, events, budget).await?;
        // Fill the budget in priority order, render in canonical order (a
        // stable order keeps shared prefixes cacheable across questions).
        let mut used = 0usize;
        let mut keep: Vec<TableId> = Vec::with_capacity(chosen.len());
        for t in chosen {
            let Some(b) = s.block(t) else { continue };
            if used + b.range.len() > budget_bytes {
                continue;
            }
            used += b.range.len();
            keep.push(t);
        }
        s.subset_into(&keep, &mut self.schema_buf);
        self.ctx.extend(s.blocks().iter().map(|b| b.table).filter(|t| keep.contains(t)));
        // How the tables join (inferred from key names).
        let mut first = true;
        for edge in e.links.edges_among(&self.ctx) {
            let (Some(fa), Some(ta)) = (s.alias(edge.from.0), s.alias(edge.to.0)) else { continue };
            let (Some(fc), Some(tc)) = (e.catalog.column(edge.from.1), e.catalog.column(edge.to.1))
            else {
                continue;
            };
            if first {
                self.schema_buf.push_str("Joins:\n");
                first = false;
            }
            let _ = writeln!(self.schema_buf, "  {fa}.{} = {ta}.{}", fc.path, tc.path);
        }
        let tokens = graph_r::text::estimate_tokens(self.schema_buf.len());
        Ok((self.ctx.len() as u32, u32::try_from(tokens).unwrap_or(u32::MAX)))
    }

    /// Link the question to tables: lexical (+ grounding boosts), then —
    /// when the model takes a JSON schema — one enum-constrained choice
    /// among the candidates' cards.
    async fn link<E: Events>(
        &mut self,
        question: &str,
        events: &mut E,
        budget: &mut Budget,
    ) -> Result<Vec<TableId>> {
        let e = self.e;
        let lc = e.cfg.link;
        let boosts: Vec<(TableId, f32)> = self
            .grounded
            .iter()
            .filter_map(|g| {
                e.catalog.column(g.col).map(|c| {
                    (c.table, if g.kind == MatchKind::Exact { BOOST_EXACT } else { BOOST_FUZZY })
                })
            })
            .collect();
        let opts = LinkOpts { max_tables: lc.max_tables, ..LinkOpts::default() };
        let lexical = e.links.link(question, &boosts, opts);
        if !lc.model || !e.model.caps().constraints.json_schema || lc.candidates < 2 {
            events.stage(Stage::Linking {
                candidates: lexical.len() as u32,
                chosen: lexical.len() as u32,
                model: false,
            });
            return Ok(lexical);
        }
        let picks = self.candidates(question, &lexical, &boosts);
        if picks.len() < 2 && e.all_cards.is_none() {
            events.stage(Stage::Linking {
                candidates: picks.len() as u32,
                chosen: lexical.len() as u32,
                model: false,
            });
            return Ok(lexical);
        }
        // Every table's card when they fit (stable, cacheable); else the
        // candidates', one card per table family, in catalog order.
        let built;
        let cards: &Cards = if let Some(all) = &e.all_cards {
            all
        } else {
            let mut reps: Vec<TableId> = Vec::with_capacity(picks.len());
            for t in picks {
                let (r, _) = e.links.family(t);
                if !reps.contains(&r) {
                    reps.push(r);
                }
            }
            reps.sort_unstable();
            built = Cards::build(&e.catalog, &e.links, &reps);
            &built
        };
        let schema = &cards.schema;
        let q = format!("Question: {question}");
        let parts = [Part::cached(&cards.text), Part::text(&q)];
        let req = GenRequest {
            constraint: Constraint::JsonSchema(schema),
            max_output_tokens: 40 + 16 * lc.max_tables as u32,
            independent: true,
            ..GenRequest::new(Prompt { system: LINK_SYSTEM, parts: &parts })
        };
        let usage = e.model.generate(&req, &mut self.cands, &mut ()).await.map_err(Error::Model)?;
        let _ = budget.charge(usage.spend());
        let mut chosen = parse_tables(self.cands.text(0), &cards.names, &cards.ids);
        if chosen.is_empty() {
            // Unusable reply: the lexical choice stands.
            chosen = lexical;
        } else {
            e.links.complete(&mut chosen, LinkOpts::default().max_hops);
        }
        events.stage(Stage::Linking {
            candidates: cards.ids.len() as u32,
            chosen: chosen.len() as u32,
            model: true,
        });
        Ok(chosen)
    }

    /// Candidate tables for the linking call: the lexical choice, its join
    /// neighbours, grounded tables, a seat per dataset, then the rest by
    /// score — at most `candidates`, one per table family.
    fn candidates(
        &self,
        question: &str,
        lexical: &[TableId],
        boosts: &[(TableId, f32)],
    ) -> Vec<TableId> {
        let e = self.e;
        let lc = e.cfg.link;
        // Candidates: the lexical choice, its join neighbours, grounded
        // tables, then the next best by score.
        let mut picks: Vec<TableId> = lexical.to_vec();
        let push = |t: TableId, picks: &mut Vec<TableId>| {
            if !picks.contains(&t) && picks.len() < lc.candidates {
                picks.push(t);
            }
        };
        for edge in e.links.edges() {
            if lexical.contains(&edge.from.0) {
                push(edge.to.0, &mut picks);
            }
            if lexical.contains(&edge.to.0) {
                push(edge.from.0, &mut picks);
            }
        }
        for &(t, _) in boosts {
            push(t, &mut picks);
        }
        // Every dataset gets a seat (its best-scoring table families, else
        // its largest): a question worded unlike any table name still sees
        // the dataset that answers it. Then the rest by score.
        let scored = e.links.score(question);
        let per_dataset = (lc.candidates / e.catalog.datasets().count().max(1)).clamp(1, 2);
        for ds in e.catalog.datasets() {
            let mut seat: Vec<TableId> = scored
                .iter()
                .map(|&(t, _)| e.links.family(t).0)
                .filter(|&t| e.catalog.table(t).is_some_and(|tb| tb.dataset_name == ds.name))
                .collect();
            seat.dedup();
            if seat.is_empty() {
                let mut by_rows: Vec<(u64, TableId)> = e
                    .links
                    .representatives()
                    .filter_map(|t| {
                        e.catalog
                            .table(t)
                            .filter(|tb| tb.dataset_name == ds.name)
                            .map(|tb| (tb.row_count, t))
                    })
                    .collect();
                by_rows.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
                seat = by_rows.into_iter().map(|(_, t)| t).collect();
            }
            for t in seat.into_iter().take(per_dataset) {
                push(t, &mut picks);
            }
        }
        for &(t, _) in &scored {
            if picks.len() >= lc.candidates {
                break;
            }
            push(e.links.family(t).0, &mut picks);
        }
        picks
    }

    /// The per-question values section: stored values the question names,
    /// as `alias.column = literal` lines, for tables in context. Exact
    /// matches on columns whose full value list is already in the schema
    /// are left out (no new information, fewer tokens).
    pub(crate) fn grounding_text(&self, out: &mut String) {
        let e = self.e;
        let mut n = 0;
        for g in &self.grounded {
            let Some(c) = e.catalog.column(g.col) else { continue };
            if !self.ctx.contains(&c.table) {
                continue;
            }
            let Some(alias) = e.schema.alias(c.table) else { continue };
            if g.kind == MatchKind::Exact && e.cfg.render.profiles {
                if let Some(p) = e.catalog.profile(g.col) {
                    if p.values_complete && p.values().count() <= e.cfg.render.max_values {
                        continue;
                    }
                }
            }
            if n == 0 {
                out.push_str(
                    "Values in the data that the question refers to (use these exact literals):\n",
                );
            }
            let _ = write!(out, "  {alias}.{} = ", c.path);
            literal_into(&g.value, c.data_type == "STRING", out);
            if g.kind != MatchKind::Exact || !g.phrase.eq_ignore_ascii_case(&g.value) {
                out.push_str("  -- \"");
                out.push_str(&g.phrase);
                out.push('"');
            }
            out.push('\n');
            n += 1;
            if n == e.cfg.max_grounded {
                break;
            }
        }
    }

    /// Build the schema grammar for the tables in context (once per
    /// distinct context; well under a millisecond for thousands of
    /// columns).
    pub(crate) fn ensure_grammar(&mut self) {
        let e = self.e;
        let Some(dialect) = e.dialect else { return };
        if self.grammar.as_ref().is_some_and(|(ctx, _)| *ctx == self.ctx) {
            return;
        }
        let mut fqns: Vec<String> = Vec::with_capacity(self.ctx.len());
        let mut cols: Vec<Vec<&str>> = Vec::with_capacity(self.ctx.len());
        let mut aliases: Vec<&str> = Vec::with_capacity(self.ctx.len());
        for &t in &self.ctx {
            let (Some(tb), Some(alias)) = (e.catalog.table(t), e.schema.alias(t)) else { continue };
            fqns.push(format!("{}.{}.{}", tb.project, tb.dataset_name, tb.name));
            aliases.push(alias);
            cols.push(
                e.catalog
                    .columns(t)
                    .filter(|c| !c.in_array() && (!c.hidden() || c.partitioning()))
                    .map(|c| c.path)
                    .collect(),
            );
        }
        let tables: Vec<crate::grammar::Table<'_>> = fqns
            .iter()
            .zip(aliases)
            .zip(cols)
            .map(|((fqn, alias), columns)| crate::grammar::Table { fqn, alias, columns })
            .collect();
        let g = crate::grammar::Grammar::sql(&tables, crate::ir::funcs::NAMES);
        let text = match dialect {
            Dialect::Lark => g.lark(),
            Dialect::Gbnf => g.gbnf(),
        };
        self.grammar = Some((self.ctx.clone(), text));
    }
}

/// Table cards for the linking call: text, the enum of names and the
/// JSON schema constraining the reply to them.
#[derive(Debug)]
pub(crate) struct Cards {
    pub(crate) text: String,
    pub(crate) names: Vec<String>,
    pub(crate) ids: Vec<TableId>,
    pub(crate) schema: String,
}

impl Cards {
    /// Cards of `tables` (family representatives, in order); a family's
    /// card says how many same-shaped tables it stands for.
    pub(crate) fn build(
        cat: &crate::catalog::Catalog,
        links: &crate::link::SchemaIndex,
        tables: &[TableId],
    ) -> Self {
        let mut text = String::with_capacity(tables.len() * 160);
        let mut names = Vec::with_capacity(tables.len());
        let mut ids = Vec::with_capacity(tables.len());
        for &t in tables {
            let Some(tb) = cat.table(t) else { continue };
            table_card(cat, t, &mut text);
            let (_, n) = links.family(t);
            if n > 1 {
                text.pop();
                let _ = writeln!(
                    text,
                    "  (+{} more tables {}.* with the same columns, sharded by name)",
                    n - 1,
                    tb.dataset_name
                );
            }
            names.push(format!("{}.{}", tb.dataset_name, tb.name));
            ids.push(t);
        }
        let mut schema =
            String::with_capacity(64 + names.iter().map(|n| n.len() + 3).sum::<usize>());
        let mut w = Writer::new(&mut schema);
        w.begin_object().key("type").string("object").key("properties").begin_object();
        w.key("tables").begin_object().key("type").string("array").key("items").begin_object();
        w.key("type").string("string").key("enum").begin_array();
        for n in &names {
            w.string(n);
        }
        w.end_array().end_object().end_object().end_object();
        w.key("required").begin_array().string("tables").end_array().end_object();
        Self { text, names, ids, schema }
    }
}

/// The tables named in a `{"tables": [...]}` reply, in reply order, mapped
/// through `names` → `ids` (unknown names — impossible under the enum, but
/// a backend may ignore constraints — are skipped).
fn parse_tables(reply: &str, names: &[String], ids: &[TableId]) -> Vec<TableId> {
    let mut out = Vec::new();
    let body = reply
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();
    let mut s = Scanner::new(body);
    let mut buf = String::new();
    let Ok(Event::ObjectStart) = s.expect() else { return out };
    while let Ok(ev) = s.expect() {
        match ev {
            Event::Key(k) if k.eq_plain("tables") => {
                if s.expect().ok() != Some(Event::ArrayStart) {
                    return out;
                }
                while let Ok(Event::Str(v)) = s.expect() {
                    let Ok(name) = v.decode(&mut buf) else { continue };
                    if let Some(i) = names.iter().position(|n| n == name) {
                        if !out.contains(&ids[i]) {
                            out.push(ids[i]);
                        }
                    }
                }
                return out;
            }
            Event::Key(_) => {
                let Ok(v) = s.expect() else { return out };
                if s.skip(v).is_err() {
                    return out;
                }
            }
            _ => return out,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_linking_replies_defensively() {
        let names = vec!["shop.orders".to_owned(), "shop.users".to_owned()];
        let ids = vec![TableId(3), TableId(9)];
        assert_eq!(
            parse_tables(
                r#"{"tables": ["shop.users", "shop.orders", "shop.users"]}"#,
                &names,
                &ids
            ),
            vec![TableId(9), TableId(3)]
        );
        assert_eq!(
            parse_tables(
                "```json\n{\"x\": 1, \"tables\": [\"shop.bogus\", \"shop.orders\"]}\n```",
                &names,
                &ids
            ),
            vec![TableId(3)]
        );
        assert!(parse_tables("not json", &names, &ids).is_empty());
    }
}
