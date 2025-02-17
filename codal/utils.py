def normalize_field(field: str) -> dict:
    """Returns a MongoDB expression to normalize a field between -1 and 1."""
    max_abs_value = {
        "$max": [{"$abs": {"$min": f"${field}"}}, {"$max": f"${field}"}]
    }

    return {
        "$divide": [
            f"${field}",
            {
                "$cond": {
                    "if": {"$eq": [max_abs_value, 0]},
                    "then": 1,  # Avoid division by zero
                    "else": max_abs_value,
                }
            },
        ]
    }
