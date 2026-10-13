// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Front-end throughput: tokenizer and parser MB/s over a representative
// GoogleSQL corpus, and the cost of stream-abort viability checks at every
// chunk of a streamed model answer.

use crab_r::sql::lexer::tokenize;
use crab_r::sql::{emit::emit, viability, Mode, Parser};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;

const CORPUS: &[&str] = &[
    "SELECT o.status, COUNT(*) AS n FROM `proj.shop.orders` AS o WHERE o.order_ts >= TIMESTAMP '2026-04-01' AND o.is_test = FALSE GROUP BY o.status ORDER BY n DESC LIMIT 10",
    "WITH first_orders AS (SELECT o.customer_id, MIN(o.order_ts) AS first_ts FROM `proj.shop.orders` AS o GROUP BY o.customer_id), repeat AS (SELECT o.customer_id FROM `proj.shop.orders` AS o JOIN first_orders AS f ON f.customer_id = o.customer_id WHERE o.order_ts > f.first_ts GROUP BY o.customer_id) SELECT c.country, COUNT(DISTINCT r.customer_id) AS repeat_customers FROM repeat AS r JOIN `proj.shop.customers` AS c ON c.id = r.customer_id GROUP BY c.country ORDER BY repeat_customers DESC",
    "SELECT d.day, SUM(li.price * li.qty) - COALESCE(SUM(rf.amount), 0) AS net_revenue, SAFE_DIVIDE(SUM(li.price * li.qty), COUNT(DISTINCT o.order_id)) AS aov FROM `proj.shop.orders` AS o JOIN `proj.shop.line_items` AS li ON li.order_id = o.order_id LEFT JOIN `proj.shop.refunds` AS rf ON rf.order_id = o.order_id CROSS JOIN UNNEST([DATE_TRUNC(DATE(o.order_ts), DAY)]) AS d_day, (SELECT d_day AS day) AS d WHERE o.status IN ('shipped', 'delivered') GROUP BY d.day QUALIFY ROW_NUMBER() OVER (ORDER BY net_revenue DESC) <= 30",
    "SELECT user_id, ARRAY_AGG(STRUCT(event_name AS name, event_ts AS ts) ORDER BY event_ts DESC LIMIT 5) AS last_events, APPROX_QUANTILES(session_seconds, 100)[OFFSET(50)] AS p50 FROM `proj.analytics.events_*` WHERE _TABLE_SUFFIX BETWEEN '20260901' AND '20260928' AND event_name NOT LIKE 'debug%' GROUP BY user_id HAVING COUNT(*) > 3",
    "SELECT CASE WHEN amount >= 1000 THEN 'large' WHEN amount >= 100 THEN 'medium' ELSE 'small' END AS bucket, EXTRACT(DAYOFWEEK FROM created_at AT TIME ZONE 'America/Toronto') AS dow, CAST(amount AS NUMERIC) AS amt FROM `proj.fin.payments` WHERE created_at BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) AND CURRENT_TIMESTAMP()",
];

fn bench(c: &mut Criterion) {
    let corpus: String = CORPUS.join(";\n");
    let total: usize = CORPUS.iter().map(|q| q.len()).sum();

    let mut g = c.benchmark_group("front_end");
    g.throughput(Throughput::Bytes(total as u64));
    let mut toks = Vec::new();
    g.bench_function("tokenize", |b| {
        b.iter(|| {
            for q in CORPUS {
                tokenize(black_box(q), &mut toks).unwrap();
            }
        });
    });
    let mut p = Parser::new();
    g.bench_function("parse", |b| {
        b.iter(|| {
            for q in CORPUS {
                black_box(p.parse(black_box(q), Mode::Full).unwrap().root);
            }
        });
    });
    let mut out = String::new();
    g.bench_function("parse+emit", |b| {
        b.iter(|| {
            for q in CORPUS {
                out.clear();
                let parsed = p.parse(black_box(q), Mode::Full).unwrap();
                emit(&parsed, &mut out);
                black_box(out.len());
            }
        });
    });
    g.finish();

    // A streamed answer checked every 16 bytes (a typical delta size).
    let answer = CORPUS[1];
    let cuts: Vec<usize> =
        (16..answer.len()).step_by(16).filter(|&i| answer.is_char_boundary(i)).collect();
    let mut g = c.benchmark_group("stream_viability");
    g.throughput(Throughput::Elements(cuts.len() as u64));
    g.bench_function("per_chunk", |b| {
        b.iter(|| {
            for &i in &cuts {
                black_box(viability(&mut p, &answer[..i], Mode::Strict));
            }
        });
    });
    g.finish();
    black_box(corpus);
}

criterion_group!(benches, bench);
criterion_main!(benches);
