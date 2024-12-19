def partition_name_sanitizer(x: str, reverse=False):
    mapping = {
        "...": "الله",
        "\x07": "/x07",
        "\x08": "/x08",
        "\x0c": "/x08",
        "\n": "/n",
        "\r": "/r",
        "\t": "/t",
        "\x0b": "/x0b",
        "\x00": "/x00",
    }
    for k, v in mapping.items():
        x = x.replace(*((v, k) if reverse else (k, v)))
    return x
