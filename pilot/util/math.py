#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

"""Common math functions."""

from decimal import Decimal
from re import split, sub
from typing import Any

from pilot.common.exception import NotDefined

SYMBOLS = {
    'customary': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa', 'zetta', 'iotta'),

    'iec': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi', 'zebi', 'yobi'),
}


def mean(data: list) -> float:
    """Return the sample arithmetic mean of *data*.

    Args:
        data: Non-empty list of numeric values.

    Returns:
        Arithmetic mean of the list.

    Raises:
        ValueError: If *data* contains fewer than one element.
    """
    n = len(data)
    if n < 1:
        raise ValueError('mean requires at least one data point')

    return sum(data) / float(n)


def sum_square_dev(data: list) -> float:
    """Return the sum of squared deviations from the mean.

    Computes ``sum((x - mean(data))**2)`` for each element ``x`` in *data*.

    Args:
        data: List of numeric values.

    Returns:
        Sum of squared deviations.
    """
    c = mean(data)

    return sum((x - c) ** 2 for x in data)


def sum_dev(x: list, y: list) -> float:
    """Return the sum of cross-deviations for two sequences.

    Computes ``sum((xi - mean(x)) * (yi - mean(y)))`` for paired elements.

    Args:
        x: First list of numeric values.
        y: Second list of numeric values (must be the same length as *x*).

    Returns:
        Sum of cross-deviations.
    """
    return sum((_x - mean(x)) * (_y - mean(y)) for _x, _y in zip(x, y))


def chi2(observed: list, expected: list) -> float:
    """Return the chi-squared sum for observed and expected value lists.

    Returns 0.0 immediately if any expected value is zero to avoid division
    by zero.

    Args:
        observed: List of observed values.
        expected: List of expected values (same length as *observed*).

    Returns:
        Chi-squared sum, or 0.0 if any expected value is zero.
    """
    if 0 in expected:
        return 0.0

    return sum((_o - _e) ** 2 / _e ** 2 for _o, _e in zip(observed, expected))


def float_to_rounded_string(num: float, precision: int = 3) -> str:
    """Convert a float to a string rounded to the given number of decimal places.

    Example::

        float_to_rounded_string(3.1415, 2) == '3.14'

    Args:
        num: Number to convert.
        precision: Number of decimal places to retain.

    Returns:
        String representation of *num* rounded to *precision* decimal places.

    Raises:
        NotDefined: If *precision* cannot be used to build a ``Decimal`` exponent,
            or if *num* cannot be converted to ``Decimal``.
    """
    try:
        _precision = Decimal(10) ** -precision
    except Exception as exc:
        raise NotDefined(f'failed to define precision={precision}: {exc}') from exc

    try:
        s = Decimal(str(num)).quantize(_precision)
    except Exception as exc:
        raise NotDefined(f'failed to convert {num} to Decimal: {exc}') from exc

    return str(s)


def tryint(x: Any) -> Any:
    """Try to convert *x* to an integer, returning the original value on failure.

    Used during version-string comparison to handle components that contain
    letters (e.g. ``'Nightly'``).

    Args:
        x: Value to convert.

    Returns:
        Integer conversion of *x*, or the original value if conversion raises
        ``ValueError``.
    """
    try:
        return int(x)
    except ValueError:
        return x


def split_version(version: str) -> tuple:
    """Split a version string into a tuple, converting numeric parts to integers.

    Non-numeric parts are left as strings. Useful for rank-based version
    comparison::

        split_version("1.2.3")       == (1, 2, 3)
        split_version("1.2.Nightly") == (1, 2, "Nightly")

    The result can also serve as a sort key::

        sorted(['YT4.11', '4.3', 'YT4.2', '4.10'], key=split_version)

    Args:
        version: Dot-separated release string.

    Returns:
        Tuple of integer and/or string components.
    """
    return tuple(tryint(x) for x in split('([^.]+)', version))


def is_greater_or_equal(num_a: str, num_b: str) -> bool:
    """Check whether version string *num_a* is greater than or equal to *num_b*.

    Comparison rules::

        "1.2.3" > "1.2"   — more digits implies greater
        "1.2.3" > "1.2.2" — rank-based comparison
        "1.3.2" > "1.2.3" — rank-based comparison
        "1.2.N" > "1.2.2" — nightly builds are always considered greater

    Args:
        num_a: First version string.
        num_b: Second version string.

    Returns:
        True if *num_a* >= *num_b*, False otherwise.
    """
    return split_version(num_a) >= split_version(num_b)


def add_lists(list1: list, list2: list) -> list:
    """Merge two lists, preserving order and removing duplicates from the second.

    Example::

        add_lists([1, 2, 3, 4], [3, 4, 5, 6]) == [1, 2, 3, 4, 5, 6]

    Args:
        list1: First input list (order is preserved).
        list2: Second input list (elements already in *list1* are dropped).

    Returns:
        Combined list with duplicates removed.
    """
    return list1 + list(set(list2) - set(list1))


def convert_mb_to_b(size: Any) -> int:
    """Convert a size value from megabytes to bytes.

    Coerces *size* to ``int`` before conversion; floats are truncated.

    Args:
        size: Size in MB. May be a float, int, or numeric string.

    Returns:
        Equivalent size in bytes.

    Raises:
        ValueError: If *size* cannot be converted to an integer.
    """
    try:
        size = int(size)
    except Exception as exc:
        raise ValueError(f'cannot convert {size} to int: {exc}') from exc

    return size * 1024 ** 2


def convert_b_to_gb(size: Any) -> int:
    """Convert a size value from bytes to gigabytes, rounded to the nearest integer.

    Coerces *size* to ``int`` before conversion.

    Args:
        size: Size in bytes. May be a float, int, or numeric string.

    Returns:
        Equivalent size in GB, rounded to the nearest integer.

    Raises:
        ValueError: If *size* cannot be converted to an integer.
    """
    try:
        size = int(size)
    except Exception as exc:
        raise ValueError(f'cannot convert {size} to int: {exc}') from exc

    return round(size / 1024**3)


def diff_lists(list_a: list, list_b: list) -> list:
    """Return elements present in *list_a* but not in *list_b*.

    Args:
        list_a: Minuend list.
        list_b: Subtrahend list.

    Returns:
        List of elements in *list_a* that are not in *list_b*.
    """
    return list(set(list_a) - set(list_b))


def bytes2human(num: Any, symbols: str = 'customary') -> str:
    """Convert *num* bytes to a human-readable string.

    *symbols* selects the unit set. Accepted values are ``'customary'``,
    ``'customary_ext'``, ``'iec'``, and ``'iec_ext'``.

    Examples::

        bytes2human(0)       == '0.0 B'
        bytes2human(1024)    == '1.0 K'
        bytes2human(1048576) == '1.0 M'

    Args:
        num: Number of bytes to convert.
        symbols: Symbol set to use for unit labels.

    Returns:
        Human-readable size string with one decimal place.

    Raises:
        ValueError: If *num* is negative or cannot be converted to an integer.
    """
    _format = '%(value).1f %(symbol)s'

    try:
        number = int(num)
    except ValueError as exc:
        raise exc
    if number < 0:
        raise ValueError("n < 0")
    symbols = SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i + 1) * 10
    for symbol in reversed(symbols[1:]):
        if number >= prefix[symbol]:
            # value = float(number) / prefix[symbol]
            return _format % locals()

    return _format % {"symbol": symbols[0], "value": number}


def human2bytes(snumber: str, divider: Any = None) -> int:
    """Convert a human-readable size string to bytes.

    Infers the unit from the string. Raises ``ValueError`` when the format
    cannot be recognised.

    Rules:

    - No digit prefix → treated as 1 of that unit (e.g. ``"KB"`` == ``"1 KB"``).
    - No letter suffix → treated as bytes (e.g. ``"512"`` == ``"512 B"``).
    - ``'k'`` is accepted as an alias for ``'K'``.
    - *divider* converts the result to another magnitude (e.g. ``"M"`` returns MB).

    Examples::

        human2bytes('0 B')    == 0
        human2bytes('1 K')    == 1024
        human2bytes('1 M')    == 1048576
        human2bytes('1 Gi')   == 1073741824
        human2bytes('1 M', 'K') == 1024

    Args:
        snumber: Human-readable size string, e.g. ``'1.5 GB'``.
        divider: Optional divisor string to convert the result to another unit.

    Returns:
        Size in bytes (or in units of *divider* when provided).

    Raises:
        ValueError: If the unit suffix cannot be recognised.
    """
    init = snumber
    num = ""
    while snumber and snumber[0:1].isdigit() or snumber[0:1] == '.':
        num += snumber[0]
        snumber = snumber[1:]

    if len(num) == 0:
        num = "1"

    try:
        number = float(num)
    except ValueError as exc:
        raise exc

    letter = snumber.strip()
    letter = sub(r'(?i)(?<=.)(bi?|bytes?)$', "", letter)
    if len(letter) == 0:
        letter = "B"

    for _, sset in list(SYMBOLS.items()):
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError(f"can't interpret {init!r}")  # = repr(init)
    prefix = {sset[0]: 1}
    for inum, snum in enumerate(sset[1:]):
        prefix[snum] = 1 << (inum + 1) * 10

    div = 1 if divider is None else human2bytes(divider)

    try:
        ret = int(number * prefix[letter] / div)
    except ValueError as exc:
        raise exc

    return ret


def convert_seconds_to_hours_minutes_seconds(seconds: int) -> tuple:
    """Convert a duration in seconds to hours, minutes, and remaining seconds.

    Args:
        seconds: Total duration in seconds.

    Returns:
        Tuple of ``(hours, minutes, remaining_seconds)``.
    """
    hours = seconds // 3600
    remaining_seconds = seconds % 3600
    minutes = remaining_seconds // 60
    remaining_seconds %= 60

    return hours, minutes, remaining_seconds
