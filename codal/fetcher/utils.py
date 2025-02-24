def sanitize_persian(input: str) -> str:
    trans = str.maketrans("۰۱۲۳۴۵۶۷۸۹يك", "0123456789یک")
    return input.translate(trans)


class APIError(Exception):
    status_code: int

    def __init__(self, message: str, status_code: int):
        self.message = message
        self.status_code = status_code
        super().__init__(message)
