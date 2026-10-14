// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! crab-describe: evidence-driven metadata for BigQuery projects,
//! datasets, tables and SQL. See [`crab_r::describe::app`] for the options.

use crab_r::describe::app;
use std::process::ExitCode;

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if let Some(cmd) = argv.first().filter(|c| app::LOOKUPS.contains(&c.as_str())) {
        return match app::lookup(cmd, &argv[1..]) {
            Ok(text) => {
                print!("{text}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        };
    }
    if argv.first().map(String::as_str) == Some("pull") {
        let (Some(src), Some(dir)) = (argv.get(1), argv.get(2)) else {
            eprintln!("usage: crab-describe pull gs://BUCKET/PREFIX|DIR STORE_DIR");
            return ExitCode::from(2);
        };
        return finish("pull", objstore_r::http::block_on(app::pull(src, dir)), |msg| {
            eprintln!("{msg}");
        });
    }
    let opts = match app::parse(argv) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let run = objstore_r::http::block_on(app::run(&opts, &mut |line| eprintln!("{line}")));
    finish("", run, |report| {
        if opts.out.is_none() {
            println!("{}", crab_r::describe::report::to_json(&report));
        }
    })
}

fn finish<T>(what: &str, r: std::io::Result<Result<T, String>>, ok: impl FnOnce(T)) -> ExitCode {
    let name =
        if what.is_empty() { "crab-describe".to_owned() } else { format!("crab-describe {what}") };
    match r {
        Ok(Ok(v)) => {
            ok(v);
            ExitCode::SUCCESS
        }
        Ok(Err(e)) => {
            eprintln!("{name}: {e}");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("{name}: runtime: {e}");
            ExitCode::FAILURE
        }
    }
}
