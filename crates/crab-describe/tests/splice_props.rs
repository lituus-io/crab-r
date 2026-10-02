// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The splicer on random schemas: the result carries exactly the requested
// descriptions (others untouched), everything that is not a description
// survives byte-for-byte, and splicing again is a no-op.

use crab_core::json::{Event, Scanner};
use crab_describe::splice::splice;
use proptest::prelude::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
struct Field {
    name: String,
    desc: Option<String>,
    extra: bool,
    children: Vec<Field>,
}

fn field(depth: u32) -> BoxedStrategy<Field> {
    let leaf = ("[a-z][a-z0-9_]{0,6}", proptest::option::of("[ -~]{0,12}"), any::<bool>())
        .prop_map(|(name, desc, extra)| Field { name, desc, extra, children: vec![] });
    if depth == 0 {
        return leaf.boxed();
    }
    (leaf, proptest::collection::vec(field(depth - 1), 0..3))
        .prop_map(|(mut f, c)| {
            f.children = c;
            f
        })
        .boxed()
}

fn render(fields: &[Field], out: &mut String) {
    out.push('[');
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str("{\"name\": ");
        crab_core::json::escape_into(&f.name, out);
        out.push_str(", \"type\": \"STRING\"");
        if let Some(d) = &f.desc {
            out.push_str(", \"description\": ");
            crab_core::json::escape_into(d, out);
        }
        if f.extra {
            out.push_str(", \"policyTags\": {\"names\": [\"t\"]}, \"mode\": \"NULLABLE\"");
        }
        if !f.children.is_empty() {
            out.push_str(", \"fields\": ");
            render(&f.children, out);
        }
        out.push('}');
    }
    out.push(']');
}

fn paths(fields: &[Field], prefix: &str, out: &mut Vec<(String, Option<String>)>) {
    for f in fields {
        let p = if prefix.is_empty() { f.name.clone() } else { format!("{prefix}.{}", f.name) };
        out.push((p.clone(), f.desc.clone()));
        paths(&f.children, &p, out);
    }
}

/// path → description, read back from JSON.
fn read(json: &str) -> BTreeMap<String, Option<String>> {
    fn walk(src: &str, prefix: &str, out: &mut BTreeMap<String, Option<String>>) {
        let mut s = Scanner::new(src);
        assert_eq!(s.expect().unwrap(), Event::ArrayStart);
        let mut buf = String::new();
        loop {
            match s.expect().unwrap() {
                Event::ArrayEnd => return,
                Event::ObjectStart => {
                    let (mut name, mut desc, mut kids) = (String::new(), None, None::<usize>);
                    loop {
                        match s.expect().unwrap() {
                            Event::ObjectEnd => break,
                            Event::Key(k) if k.eq_plain("name") => {
                                if let Event::Str(v) = s.expect().unwrap() {
                                    name = v.decode(&mut buf).unwrap().to_owned();
                                }
                            }
                            Event::Key(k) if k.eq_plain("description") => {
                                if let Event::Str(v) = s.expect().unwrap() {
                                    desc = Some(v.decode(&mut buf).unwrap().to_owned());
                                }
                            }
                            Event::Key(k) if k.eq_plain("fields") => {
                                let at = s.offset();
                                kids = Some(at + src[at..].find('[').unwrap());
                                let v = s.expect().unwrap();
                                s.skip(v).unwrap();
                            }
                            Event::Key(_) => {
                                let v = s.expect().unwrap();
                                s.skip(v).unwrap();
                            }
                            _ => panic!(),
                        }
                    }
                    let p =
                        if prefix.is_empty() { name.clone() } else { format!("{prefix}.{name}") };
                    out.entry(p.clone()).or_insert(desc);
                    if let Some(at) = kids {
                        walk(&src[at..], &p, out);
                    }
                }
                _ => panic!(),
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(json, "", &mut out);
    out
}

/// The JSON with every description member removed (to compare the rest).
fn strip_descriptions(json: &str) -> String {
    let mut out = String::new();
    let mut rest = json;
    while let Some(i) = rest.find("\"description\"") {
        // Cut the member and the separator before it.
        let head = &rest[..i];
        let head = head.trim_end().strip_suffix(',').unwrap_or(head);
        out.push_str(head);
        let after = &rest[i + "\"description\"".len()..];
        let mut sc = Scanner::new(after.trim_start_matches([':', ' ']));
        let v = sc.expect().unwrap();
        sc.skip(v).unwrap();
        let consumed = after.len() - after.trim_start_matches([':', ' ']).len() + sc.offset();
        rest = &after[consumed..];
    }
    out.push_str(rest);
    out.replace(' ', "")
}

proptest! {
    #[test]
    fn splice_changes_only_requested_descriptions(fields in proptest::collection::vec(field(2), 1..5), picks in proptest::collection::vec(any::<prop::sample::Index>(), 0..6), texts in proptest::collection::vec("[ -~\n\"\\\\]{1,20}", 6)) {
        let mut json = String::new();
        render(&fields, &mut json);
        let mut all = Vec::new();
        paths(&fields, "", &mut all);
        // Only paths that are unique (duplicate names at one level are ambiguous).
        let mut updates = BTreeMap::new();
        for (i, pick) in picks.iter().enumerate() {
            let (p, _) = &all[pick.index(all.len())];
            if all.iter().filter(|(q, _)| q == p).count() == 1 {
                updates.insert(p.to_ascii_lowercase(), texts[i].clone());
            }
        }
        let (out, _) = splice(&json, &updates).unwrap();
        let before = read(&json);
        let after = read(&out);
        for (p, d) in &before {
            match updates.get(&p.to_ascii_lowercase()).filter(|_| all.iter().filter(|(q, _)| q == p).count() == 1) {
                Some(new) => {
                    let got = after.get(p).cloned().flatten();
                    prop_assert_eq!(got.as_deref(), Some(new.as_str()));
                }
                None => prop_assert_eq!(after.get(p), Some(d)),
            }
        }
        prop_assert_eq!(strip_descriptions(&out), strip_descriptions(&json), "only descriptions changed");
        let (again, _) = splice(&out, &updates).unwrap();
        prop_assert_eq!(again, out);
    }
}
