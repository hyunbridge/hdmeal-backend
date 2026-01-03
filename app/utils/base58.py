from decimal import Decimal

_ALPHABET = "123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ"
_BASE_COUNT = len(_ALPHABET)


def encode(num: int) -> str:
    if num < 0:
        raise ValueError("Number must be non-negative")

    encoded = ""
    current = num
    while current >= _BASE_COUNT:
        mod = current % _BASE_COUNT
        encoded = _ALPHABET[int(mod)] + encoded
        current = int(Decimal(current) / _BASE_COUNT)

    if current:
        encoded = _ALPHABET[int(current)] + encoded

    return encoded or _ALPHABET[0]


def decode(value: str) -> int:
    decoded = 0
    multiplier = 1
    for char in reversed(value):
        decoded += multiplier * _ALPHABET.index(char)
        multiplier *= _BASE_COUNT
    return decoded
