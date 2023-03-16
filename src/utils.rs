use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use ammonia::Builder;
use maplit::hashset;

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
    s.replace('-', "").replace('~', "").to_lowercase()
}

pub fn fix_annotation_text(text: &str) -> String {
    let temp_text = text.replace("<br>", "\n").replace("\\n", "\n");

    let tags = hashset!["a"];
    Builder::new()
        .tags(tags)
        .clean(&temp_text)
        .to_string()
}
