import re

import numpy as np
from dagster import get_dagster_logger


def sanitize_persian(input: str) -> str:
    trans = str.maketrans("۰۱۲۳۴۵۶۷۸۹يك", "0123456789یک")
    return input.translate(trans)


class APIError(Exception):
    status_code: int

    def __init__(self, message: str, status_code: int):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


def replace_cell_references(formula, values):
    """
    Replaces all cell references (e.g., A21, B3)
    in a formula with values from a dictionary.

    :param formula: str - The formula containing
    Excel-style references.
    :param values: dict - A dictionary mapping cell names
    (e.g., "A21") to numerical values.
    :return: str - The formula with replaced values.
    """

    def replace_match(match):
        cell = match.group(0)  # Extract matched cell reference
        return str(values.get(cell, 0))  # Replace with value or default to 0

    # Match cell references (e.g., A1, B23, Z100)
    pattern = r"[A-Z]+\d+"
    return re.sub(pattern, replace_match, formula)


def replace_ranges(formula, values):
    """
    Replaces Excel-style ranges (e.g., A21:A44)
    with a comma-separated list of values.

    :param formula: str - The formula
    containing Excel-style ranges.
    :param values: dict - A dictionary mapping
    cell names (e.g., "A21") to numerical values.
    :return: str - The formula with replaced ranges.
    """

    def replace_match(match):
        start_cell, end_cell = match.groups()

        # Extract column and row numbers
        start_col, start_row = re.match(r"([A-Z]+)(\d+)", start_cell).groups()
        end_col, end_row = re.match(r"([A-Z]+)(\d+)", end_cell).groups()

        # Ensure it's the same column
        if start_col != end_col:
            raise ValueError("Multi-column ranges are not supported")

        # Generate values for the range
        start_row, end_row = int(start_row), int(end_row)
        range_values = [
            str(values.get(f"{start_col}{r}", 0))
            for r in range(start_row, end_row + 1)
        ]

        return f"({', '.join(range_values)})"

    # Match range patterns like A1:A10
    pattern = r"([A-Z]+\d+)[:,]([A-Z]+\d+)"
    return re.sub(pattern, replace_match, formula)


def is_float(value):
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def eval_formula(formula: str, address: str, values: dict[str, float] = {}):
    formula = formula.replace("=", "")
    logger = get_dagster_logger()

    def IF(condition, true_value, false_value=0):
        return true_value if condition else false_value

    def ROUND(value, digits):
        return round(value, digits)

    def SUM(*args):
        if isinstance(args[0], tuple):
            return sum(args[0])
        return sum(args)

    def ABS(value):
        return abs(value)

    pre_tranform = formula
    formula = replace_ranges(formula, values)
    formula = replace_cell_references(formula, values)
    try:
        values[address] = eval(
            formula
        )  # TODO: check if this should be overridden
        return values[address]
    except ZeroDivisionError:
        return np.nan
    except SyntaxError as e:
        logger.warning(
            f"SyntaxError: {e} | {address} : ={pre_tranform} => {formula}"
        )
        return np.nan
