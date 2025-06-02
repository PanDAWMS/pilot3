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
    """
    Return the sample arithmetic mean of data.

    :param data: list of floats or ints (list)
    :return: mean value (float).
    """
    n = len(data)
    if n < 1:
        raise ValueError('mean requires at least one data point')

    return sum(data) / float(n)


def sum_square_dev(data: list) -> float:
    """
    Return sum of square deviations of sequence data.

    Sum (x - x_mean)**2

    :param data: list of floats or ints.
    :return: sum of squares (float).
    """
    c = mean(data)

    return sum((x - c) ** 2 for x in data)


def sum_dev(x: list, y: list) -> float:
    """
    Return sum of deviations of sequence data.

    Sum (x - x_mean)**(y - y_mean)

    :param x: list of ints or floats (list)
    :param y: list of ints or floats (list)
    :return: sum of deviations (float).
    """
    return sum((_x - mean(x)) * (_y - mean(y)) for _x, _y in zip(x, y))


def chi2(observed: list, expected: list) -> float:
    """
    Return the chi2 sum of the provided observed and expected values.

    :param observed: list of floats (list)
    :param expected: list of floats (list)
    :return: chi2 (float).
    """
    if 0 in expected:
        return 0.0

    return sum((_o - _e) ** 2 / _e ** 2 for _o, _e in zip(observed, expected))


def float_to_rounded_string(num: float, precision: int = 3) -> str:
    """
    Convert float to a string with a desired number of digits (the precision).

    E.g. num=3.1415, precision=2 -> '3.14'.

    round_to_n = lambda x, n: x if x == 0 else round(x, -int(math.floor(math.log10(abs(x)))) + (n - 1))
      round_to_n(x=0.123,n=2)
      0.12

    :param num: number to be converted (float)
    :param precision: number of desired digits (int)
    :raises NotDefined: for undefined precisions and float conversions to Decimal
    :return: rounded string (str).
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
    """
    Try to convert given number to integer.

    Used by numbered string comparison (to protect against unexpected letters in version number).

    :param x: possible int (Any)
    :return: converted int or original value in case of ValueError (Any).
    """
    try:
        return int(x)
    except ValueError:
        return x


def split_version(version: str) -> tuple:
    """
    Split version string into parts and convert the parts into integers when possible.

    Any encountered strings are left as they are.
    The function is used with release strings.
    split_version("1.2.3") = (1,2,3)
    split_version("1.2.Nightly") = (1,2,"Nightly")

    The function can also be used for sorting:
    > names = ['YT4.11', '4.3', 'YT4.2', '4.10', 'PT2.19', 'PT2.9']
    > sorted(names, key=splittedname)
    ['4.3', '4.10', 'PT2.9', 'PT2.19', 'YT4.2', 'YT4.11']

    :param version: release string (str)
    :return: converted release tuple (tuple).
    """
    return tuple(tryint(x) for x in split('([^.]+)', version))


def is_greater_or_equal(num_a: str, num_b: str) -> bool:
    """
    Check if the numbered string num_a >= num_b.

    "1.2.3" > "1.2"  -- more digits
    "1.2.3" > "1.2.2"  -- rank based comparison
    "1.3.2" > "1.2.3"  -- rank based comparison
    "1.2.N" > "1.2.2"  -- nightlies checker, always greater

    :param num_a: numbered string (str)
    :param num_b: numbered string (str)
    :return: True if num_a >= num_b, False otherwise (bool).
    """
    return split_version(num_a) >= split_version(num_b)


def add_lists(list1: list, list2: list) -> list:
    """
    Add list1 and list2 and remove any duplicates.

    Example:
    list1=[1,2,3,4]
    list2=[3,4,5,6]
    add_lists(list1, list2) = [1, 2, 3, 4, 5, 6]

    :param list1: input list 1 (list)
    :param list2: input list 2 (list)
    :return: added lists with removed duplicates (list).
    """
    return list1 + list(set(list2) - set(list1))


def convert_mb_to_b(size: Any) -> int:
    """
    Convert value from MB to B for the given size variable.

    If the size is a float, the function will convert it to int.

    :param size: size in MB (float or int) (Any)
    :raises: ValueError for conversion error.
    :return: size in B (int).
    """
    try:
        size = int(size)
    except Exception as exc:
        raise ValueError(f'cannot convert {size} to int: {exc}') from exc

    return size * 1024 ** 2


def convert_b_to_gb(size: Any) -> int:
    """
    Convert value from B to GB for the given size variable.

    If the size is a float, the function will convert it to int.

    :param size: size in B (float or int) (Any)
    :raises: ValueError for conversion error.
    :return: size in GB (int).
    """
    try:
        size = int(size)
    except Exception as exc:
        raise ValueError(f'cannot convert {size} to int: {exc}') from exc

    return round(size / 1024**3)


def diff_lists(list_a: list, list_b: list) -> list:
    """
    Return the difference between list_a and list_b.

    :param list_a: input list a (list)
    :param list_b: input list b (list)
    :return: difference (list).
    """
    return list(set(list_a) - set(list_b))


def bytes2human(num: Any, symbols: str = 'customary') -> str:
    """
    Convert `num` bytes into a human-readable string based on format.

    Symbols can be either "customary", "customary_ext", "iec" or "iec_ext",
    see: http://goo.gl/kTQMs

      >>> bytes2human(0)
      '0.0 B'
      >>> bytes2human(0.9)
      '0.0 B'
      >>> bytes2human(1)
      '1.0 B'
      >>> bytes2human(1.9)
      '1.0 B'
      >>> bytes2human(1024)
      '1.0 K'
      >>> bytes2human(1048576)
      '1.0 M'
      >>> bytes2human(1099511627776127398123789121)
      '909.5 Y'

      >>> bytes2human(9856, symbols="customary")
      '9.6 K'
      >>> bytes2human(9856, symbols="customary_ext")
      '9.6 kilo'
      >>> bytes2human(9856, symbols="iec")
      '9.6 Ki'
      >>> bytes2human(9856, symbols="iec_ext")
      '9.6 kibi'

      >>> bytes2human(10000, "%(value).1f %(symbol)s/sec")
      '9.8 K/sec'

      >>> # precision can be adjusted by playing with %f operator
      >>> bytes2human(10000, _format="%(value).5f %(symbol)s")
      '9.76562 K'

    :param num: input number (Any)
    :param symbols: symbold string (str)
    :return: human-readable string (str).
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
    """
    Guess the string format based on default symbols set and return the corresponding bytes as an integer.

    When unable to recognize the format, a ValueError is raised.

    If no digit passed, only a letter, it is interpreted as a one of a kind. Eg "KB" = "1 KB".
    If no letter passed, it is assumed to be in bytes. Eg "512" = "512 B"

    The second argument is used to convert to another magnitude (eg return not bytes but KB).
    It can be interpreted as a cluster size. Eg "512 B", or "0.2 K".

      >>> human2bytes('0 B')
      0
      >>> human2bytes('3')
      3
      >>> human2bytes('K')
      1024
      >>> human2bytes('1 K')
      1024
      >>> human2bytes('1 M')
      1048576
      >>> human2bytes('1 Gi')
      1073741824
      >>> human2bytes('1 tera')
      1099511627776

      >>> human2bytes('0.5kilo')
      512
      >>> human2bytes('0.1  byte')
      0
      >>> human2bytes('1 k')  # k is an alias for K
      1024
      >>> human2bytes('12 foo')
      Traceback (most recent call last):
          ...
      ValueError: can't interpret '12 foo'

      >>> human2bytes('1 M', 'K')
      1024
      >>> human2bytes('2 G', 'M')
      2048
      >>> human2bytes('G', '2M')
      512

    :param snumber: number string (str)
    :param divider: divider (Any)
    :return: converted integer (int)
    :raises ValueError: for conversion error.
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
    """
    Convert seconds to hours, minutes, and remaining seconds.

    :param seconds: seconds (int)
    :return: hours, minutes, remaining seconds (tuple).
    """
    hours = seconds // 3600
    remaining_seconds = seconds % 3600
    minutes = remaining_seconds // 60
    remaining_seconds %= 60

    return hours, minutes, remaining_seconds
