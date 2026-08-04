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

pub fn remove_wrong_chars(s: &str) -> String {
    s.replace(';', "")
        .replace('\n', " ")
        .replace('ё', "е")
        .replace("\\\"", "\"")
        .replace("\\'", "'")
}

pub fn parse_lang(s: &str) -> String {
    s.replace(['-', '~'], "").to_lowercase()
}

pub fn fix_annotation_text(text: &str) -> String {
    let mut temp_text = text
        .replace("<br>", "\n")
        .replace("\\n", "\n")
        .replace("\\\"", "\"")
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
    use crate::utils::fix_annotation_text;

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
