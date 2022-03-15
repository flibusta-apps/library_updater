import re


def remove_wrong_ch(s: str) -> str:
    return s.replace(";", "").replace("\n", " ").replace("ё", "е")


def remove_dots(s: str) -> str:
    return s.replace(".", "")


tags_regexp = re.compile(r"<.*?>")


def fix_annotation_text(text: str) -> str:
    replace_map = {
        "&nbsp;": "",
        "[b]": "",
        "[/b]": "",
        "[hr]": "",
    }

    t = tags_regexp.sub("", text)

    for key in replace_map:
        t = t.replace(key, replace_map[key])

    return t
