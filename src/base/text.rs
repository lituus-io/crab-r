// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! [`edit_distance`]: the one Levenshtein distance behind identifier
//! suggestions ("did you mean") and value grounding.

/// Levenshtein distance between `a` and `b` under `eq`, by char, with one
/// reusable row and an early exit: any result above `max` is `max + 1`.
/// Pass `usize::MAX - 1` for an unbounded distance.
pub fn edit_distance(
    a: &str,
    b: &str,
    max: usize,
    row: &mut Vec<usize>,
    eq: impl Fn(char, char) -> bool,
) -> usize {
    let bl = b.chars().count();
    if a.chars().count().abs_diff(bl) > max {
        return max + 1;
    }
    row.clear();
    row.extend(0..=bl);
    for (i, ca) in a.chars().enumerate() {
        let mut diag = row[0];
        row[0] = i + 1;
        let mut row_min = row[0];
        for (j, cb) in b.chars().enumerate() {
            let up = row[j + 1];
            row[j + 1] = (up + 1).min(row[j] + 1).min(diag + usize::from(!eq(ca, cb)));
            diag = up;
            row_min = row_min.min(row[j + 1]);
        }
        if row_min > max {
            return max + 1;
        }
    }
    row[bl]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances() {
        let mut row = Vec::new();
        let exact = |a: &str, b: &str, max: usize, row: &mut Vec<usize>| {
            edit_distance(a, b, max, row, |x, y| x == y)
        };
        assert_eq!(exact("statuz", "status", 3, &mut row), 1);
        assert_eq!(exact("Status", "status", 3, &mut row), 1);
        assert_eq!(
            edit_distance("Status", "status", 3, &mut row, |x, y| x.eq_ignore_ascii_case(&y)),
            0
        );
        assert_eq!(exact("abc", "xyzxyz", 2, &mut row), 3);
        assert_eq!(exact("", "ab", 5, &mut row), 2);
        assert_eq!(exact("café", "cafe", usize::MAX - 1, &mut row), 1);
        assert_eq!(exact("kitten", "sitting", usize::MAX - 1, &mut row), 3);
    }
}
