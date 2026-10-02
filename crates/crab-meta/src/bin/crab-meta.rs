// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! `crab-meta`: look things up in a metadata image.
//!
//! ```text
//! crab-meta IMAGE table   project.dataset.table
//! crab-meta IMAGE column  project.dataset.table.column
//! crab-meta IMAGE upstream   project.dataset.table.column [DEPTH]
//! crab-meta IMAGE downstream project.dataset.table.column [DEPTH]
//! crab-meta IMAGE stats
//! ```

use crab_meta::{Bytes, Column, MetaImage, Walk};
use std::process::ExitCode;

const USAGE: &str = "usage: crab-meta IMAGE (table NAME | column NAME | upstream NAME [DEPTH] | downstream NAME [DEPTH] | stats)";

fn full(c: &Column<'_>) -> String {
    format!("{}.{}", c.table().map_or("?", |t| t.name()), c.path())
}

fn show_column(img: &MetaImage, c: &Column<'_>) {
    println!("{}  {}", full(c), c.data_type());
    if !c.description().is_empty() {
        println!("  description: {}", c.description());
    }
    let conf = c.confidence().as_str();
    println!(
        "  provenance: {}{}",
        c.provenance().as_str(),
        if conf.is_empty() { String::new() } else { format!(" ({conf} confidence)") }
    );
    println!("  transform: {}", c.transform().as_str());
    if !c.expression().is_empty() {
        println!("  expression: {}", c.expression());
    }
    for (p, exact) in c.parent_edges() {
        if let Some(p) = img.column_at(p) {
            println!("  from {}{}", full(&p), if exact { " (exact copy)" } else { "" });
        }
    }
    for (k, exact) in c.child_edges() {
        if let Some(k) = img.column_at(k) {
            println!("  feeds {}{}", full(&k), if exact { " (exact copy)" } else { "" });
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(path), Some(cmd)) = (args.first(), args.get(1)) else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let img = match std::fs::read(path)
        .map_err(|e| e.to_string())
        .and_then(|b| MetaImage::from_bytes(Bytes::from(b)).map_err(|e| e.to_string()))
    {
        Ok(i) => i,
        Err(e) => {
            eprintln!("{path}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let name = args.get(2).map(String::as_str).unwrap_or("");
    let depth: u32 = args.get(3).and_then(|d| d.parse().ok()).unwrap_or(10);
    match cmd.as_str() {
        "stats" => println!(
            "{} tables, {} columns, {} bytes",
            img.table_count(),
            img.column_count(),
            img.bytes().len()
        ),
        "table" => {
            let Some(t) = img.table(name) else {
                eprintln!("no table {name}");
                return ExitCode::FAILURE;
            };
            println!("{} ({}, {} rows)", t.name(), t.kind(), t.rows());
            if !t.description().is_empty() {
                println!("  description: {}", t.description());
            }
            if !t.statement_type().is_empty() {
                println!("  produced by: {}", t.statement_type());
            }
            for c in t.columns() {
                println!(
                    "  {} {} [{}] {}",
                    c.path(),
                    c.data_type(),
                    c.provenance().as_str(),
                    c.description()
                );
            }
        }
        "column" => match img.column_by_name(name) {
            Some(c) => show_column(&img, &c),
            None => {
                eprintln!("no column {name}");
                return ExitCode::FAILURE;
            }
        },
        "upstream" | "downstream" => {
            let Some(c) = img.column_by_name(name) else {
                eprintln!("no column {name}");
                return ExitCode::FAILURE;
            };
            let mut w = Walk::new();
            let mut print = |h: crab_meta::Hop| {
                if let Some(x) = img.column_at(h.column) {
                    println!(
                        "{}{}{}{}",
                        "  ".repeat(h.depth as usize),
                        full(&x),
                        if h.exact_chain { "  (exact copy)" } else { "" },
                        if x.description().is_empty() {
                            String::new()
                        } else {
                            format!("  — {}", x.description())
                        }
                    );
                }
                true
            };
            println!("{}", full(&c));
            if cmd == "upstream" {
                img.upstream(c.id(), depth, &mut w, &mut print);
            } else {
                img.downstream(c.id(), depth, &mut w, &mut print);
            }
        }
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    }
    ExitCode::SUCCESS
}
