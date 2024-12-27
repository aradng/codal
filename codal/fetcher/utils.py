def sanitize_persian(input: str) -> str:
    trans = str.maketrans("۰۱۲۳۴۵۶۷۸۹يك", "0123456789یک")
    return input.translate(trans)
