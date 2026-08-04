use ammonia::Builder;
use maplit::hashset;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

/// Reads `filename` line by line. Each line is buffered fully in memory
/// (acceptable for current dump sizes, but note multi-megabyte lines are
/// held entirely in RAM); invalid UTF-8 anywhere in the file surfaces as an
/// `io::Error` when the corresponding item in the returned iterator is read.
pub fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

/// Unescape a MySQL/MariaDB string literal (as produced by dump `INSERT`
/// statements) into the literal text it represents. This is the single
/// place responsible for undoing dump-level backslash escaping; callers
/// must not additionally hand-roll `.replace("\\\"", ...)` etc.
///
/// Handles every escape sequence MySQL/MariaDB recognizes in string
/// literals (see the MySQL manual's "String Literals" section):
/// `\0`, `\'`, `\"`, `\b`, `\n`, `\r`, `\t`, `\Z`, `\\`. `\%` and `\_` are
/// left with the backslash intact (they are only meaningful to `LIKE`
/// pattern matching, and MySQL itself preserves the backslash for these
/// two in non-pattern contexts). Any other `\<char>` sequence has the
/// backslash dropped and the character kept as-is, matching MySQL's
/// documented behavior ("For all other escape sequences, backslash is
/// ignored"). A trailing, unpaired backslash at the end of the string is
/// kept as a literal backslash.
pub fn unescape_mysql_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();

    while let Some(c) = chars.next() {
        if c != '\\' {
            result.push(c);
            continue;
        }

        match chars.next() {
            Some('0') => result.push('\0'),
            Some('\'') => result.push('\''),
            Some('"') => result.push('"'),
            Some('b') => result.push('\u{8}'),
            Some('n') => result.push('\n'),
            Some('r') => result.push('\r'),
            Some('t') => result.push('\t'),
            Some('Z') => result.push('\u{1a}'),
            Some('\\') => result.push('\\'),
            Some(other @ ('%' | '_')) => {
                result.push('\\');
                result.push(other);
            }
            Some(other) => result.push(other),
            None => result.push('\\'),
        }
    }

    result
}

/// Clean up a name-like field (author name, book title, sequence name)
/// parsed from a dump. Unescapes MySQL string-literal escapes (see
/// `unescape_mysql_string`) and folds literal newlines to spaces so
/// multi-line garbage in these fields renders sanely.
///
/// Deliberately does **not** delete `;` or fold `ё`→`е`: the stored value
/// must match the source dump byte-for-byte aside from documented
/// unescaping. Search-time normalization (e.g. `ё`→`е` folding for typo
/// tolerance) belongs in the search-indexing service (Meilisearch), not
/// in the canonical Postgres store.
pub fn remove_wrong_chars(s: &str) -> String {
    unescape_mysql_string(s).replace('\n', " ")
}

/// Normalize a BCP-47-ish language tag from a dump to its primary
/// subtag, lowercased (e.g. `ru-RU` -> `ru`, `ru_RU` -> `ru`, `RU` -> `ru`).
/// Dump values have been observed using `-`, `_` and `~` as subtag
/// separators; splitting (rather than deleting the separator) avoids
/// collapsing `ru-RU` into the nonsensical `ruru`, which would never
/// match a whitelist of primary subtags like `ru`/`be`/`uk`.
pub fn parse_lang(s: &str) -> String {
    s.split(['-', '_', '~'])
        .next()
        .unwrap_or("")
        .trim()
        .to_lowercase()
}

pub fn fix_annotation_text(text: &str) -> String {
    let unescaped = unescape_mysql_string(text);
    let mut temp_text = unescaped
        .replace("<br>", "\n")
        .replace("&nbsp;", " ")
        .replace("\u{00A0}", " ");

    while temp_text.contains("  ") {
        temp_text = temp_text.replace("  ", " ");
    }

    let tags = hashset!["a"];
    Builder::new().tags(tags).clean(&temp_text).to_string()
}

#[cfg(test)]
mod tests {
    use crate::utils::{
        fix_annotation_text, parse_lang, remove_wrong_chars, unescape_mysql_string,
    };

    // --- unescape_mysql_string ---

    #[test]
    fn unescape_leaves_plain_text_unchanged() {
        assert_eq!(unescape_mysql_string("hello world"), "hello world");
    }

    #[test]
    fn unescape_handles_double_quote() {
        assert_eq!(unescape_mysql_string("a\\\"b"), "a\"b");
    }

    #[test]
    fn unescape_handles_single_quote() {
        assert_eq!(unescape_mysql_string("a\\'b"), "a'b");
    }

    #[test]
    fn unescape_handles_newline() {
        assert_eq!(unescape_mysql_string("a\\nb"), "a\nb");
    }

    #[test]
    fn unescape_handles_carriage_return() {
        assert_eq!(unescape_mysql_string("a\\rb"), "a\rb");
    }

    #[test]
    fn unescape_handles_tab() {
        assert_eq!(unescape_mysql_string("a\\tb"), "a\tb");
    }

    #[test]
    fn unescape_handles_nul() {
        assert_eq!(unescape_mysql_string("a\\0b"), "a\0b");
    }

    #[test]
    fn unescape_handles_backspace_and_ctrl_z() {
        assert_eq!(unescape_mysql_string("a\\bb"), "a\u{8}b");
        assert_eq!(unescape_mysql_string("a\\Zb"), "a\u{1a}b");
    }

    #[test]
    fn unescape_handles_literal_backslash() {
        assert_eq!(unescape_mysql_string("a\\\\b"), "a\\b");
    }

    #[test]
    fn unescape_keeps_backslash_for_percent_and_underscore() {
        assert_eq!(unescape_mysql_string("a\\%b"), "a\\%b");
        assert_eq!(unescape_mysql_string("a\\_b"), "a\\_b");
    }

    #[test]
    fn unescape_drops_backslash_for_unknown_escape() {
        assert_eq!(unescape_mysql_string("a\\xb"), "axb");
    }

    #[test]
    fn unescape_keeps_trailing_unpaired_backslash() {
        assert_eq!(unescape_mysql_string("a\\"), "a\\");
    }

    #[test]
    fn unescape_handles_adjacent_escapes() {
        assert_eq!(unescape_mysql_string("\\n\\t\\\"\\'"), "\n\t\"'");
    }

    #[test]
    fn unescape_handles_empty_string() {
        assert_eq!(unescape_mysql_string(""), "");
    }

    // --- remove_wrong_chars ---

    #[test]
    fn remove_wrong_chars_preserves_semicolon() {
        assert_eq!(remove_wrong_chars("Foo; Bar"), "Foo; Bar");
    }

    #[test]
    fn remove_wrong_chars_preserves_yo() {
        assert_eq!(remove_wrong_chars("Ёжик в тумане"), "Ёжик в тумане");
        assert_eq!(remove_wrong_chars("ёлка"), "ёлка");
    }

    #[test]
    fn remove_wrong_chars_folds_literal_newline_to_space() {
        assert_eq!(remove_wrong_chars("Foo\nBar"), "Foo Bar");
    }

    #[test]
    fn remove_wrong_chars_unescapes_quotes() {
        assert_eq!(remove_wrong_chars("O\\'Brien"), "O'Brien");
        assert_eq!(remove_wrong_chars("say \\\"hi\\\""), "say \"hi\"");
    }

    // --- parse_lang ---

    #[test]
    fn parse_lang_passthrough_lowercase() {
        assert_eq!(parse_lang("ru"), "ru");
        assert_eq!(parse_lang("be"), "be");
        assert_eq!(parse_lang("uk"), "uk");
    }

    #[test]
    fn parse_lang_uppercase_is_lowercased() {
        assert_eq!(parse_lang("RU"), "ru");
    }

    #[test]
    fn parse_lang_hyphenated_subtag_takes_primary() {
        assert_eq!(parse_lang("ru-RU"), "ru");
    }

    #[test]
    fn parse_lang_underscore_subtag_takes_primary() {
        assert_eq!(parse_lang("ru_RU"), "ru");
    }

    #[test]
    fn parse_lang_tilde_suffix_takes_primary() {
        assert_eq!(parse_lang("ru~1"), "ru");
    }

    #[test]
    fn parse_lang_empty_string() {
        assert_eq!(parse_lang(""), "");
    }

    #[test]
    fn test_fix_annotation_text_remove_extra_spaces() {
        let input = "    ";
        let expected_result = " ";

        let result = fix_annotation_text(input);

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_fix_annotation_text_replace_br() {
        let input = "a<br>b";
        let expected_result = "a\nb";

        let result = fix_annotation_text(input);

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_fix_annotation_text_replace_nbsp() {
        let input = "a&nbsp;b";
        let expected_result = "a b";

        let result = fix_annotation_text(input);

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_fix_annotation_text_replace_unicode_nbsp() {
        let input = "a\u{00A0}b";
        let expected_result = "a b";

        let result = fix_annotation_text(input);

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_fix_annotation_text_extra_slashes() {
        let input = "a \\n b \\\"";
        let expected_result = "a \n b \"";

        let result = fix_annotation_text(input);

        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_fix_annotation_text_large() {
        let input = "\n    <p class=book>Этот роман уже стал культовым.\n    <p class=book>Это — одна из самых читаемых книг русскоязычного Интернета, по количеству скачивании соперничающая с «Метро 2033» Глуховского и «Мародером» Беркема аль Атоми.\n    <p class=book>Это — лучшая антиутопия о надвигающейся гражданской войне.\n    <p class=book>Ближайшее будущее. Русофобская политика «оранжевых» разрывает Украину надвое. «Западенцы» при поддержке НАТО пытаются силой усмирить Левобережье. Восточная Малороссия отвечает оккупантам партизанской войной. Наступает беспощадная «эпоха мертворожденных»…\n   ";
        let expected_result = "\n Этот роман уже стал культовым.\n Это — одна из самых читаемых книг русскоязычного Интернета, по количеству скачивании соперничающая с «Метро 2033» Глуховского и «Мародером» Беркема аль Атоми.\n Это — лучшая антиутопия о надвигающейся гражданской войне.\n Ближайшее будущее. Русофобская политика «оранжевых» разрывает Украину надвое. «Западенцы» при поддержке НАТО пытаются силой усмирить Левобережье. Восточная Малороссия отвечает оккупантам партизанской войной. Наступает беспощадная «эпоха мертворожденных»…\n ";

        let result = fix_annotation_text(input);

        assert_eq!(result, expected_result);
    }
}
