// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Table resources from the API through table_meta's answer and its schema
// splice: any bytes (deep nesting, huge arrays, odd members) give a typed
// error or an answer that is valid JSON, within the column and depth caps,
// never a panic; a schema with nothing to omit and no cap reached comes
// back byte-identical.

#![no_main]

use crab_r::base::json::Scanner;
use crab_r::exec::table_meta::{answer, trim_schema, Limits, MetaOptions};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let limits = Limits { max_columns: 50, max_depth: 4, max_description: 16, max_resource_bytes: 1 << 20 };
    let opts = MetaOptions { include_view_sql: true, omit_columns: &["a", "rec.x"], limits };
    if let Ok(out) = answer(data, &opts) {
        let mut s = Scanner::new(&out);
        let first = s.expect().expect("an answer is JSON");
        s.skip(first).expect("an answer is JSON");
    }
    let Ok(src) = std::str::from_utf8(data) else { return };
    if let Ok(t) = trim_schema(src, &["a".into(), "rec.x".into()], &limits) {
        assert!(t.columns <= limits.max_columns);
        let mut s = Scanner::new(&t.json);
        if let Ok(first) = s.expect() {
            s.skip(first).expect("a trimmed schema is JSON");
        }
        let again = trim_schema(&t.json, &["a".into(), "rec.x".into()], &limits).expect("its own output");
        assert_eq!(again.omitted, 0, "trimming twice removed more");
    }
    let roomy = Limits { max_columns: usize::MAX, max_depth: usize::MAX, max_description: usize::MAX, ..limits };
    if let Ok(t) = trim_schema(src, &[], &roomy) {
        assert_eq!(t.json, src, "nothing to do changed the bytes");
    }
});
