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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2026

"""Functions for performing analytics including fitting of data."""

from __future__ import annotations
import logging
from typing import Any, Union
from .services import Services
from pilot.common.exception import NotDefined, NotSameLength, UnknownException
from pilot.util.filehandling import get_table_from_file
from pilot.util.math import mean, sum_square_dev, sum_dev, chi2, float_to_rounded_string

logger = logging.getLogger(__name__)


class Analytics(Services):
    """Analytics service class."""

    _fit = None

    def __init__(self, **kwargs: Any) -> None:
        """Initialize variables.

        Args:
            **kwargs: Arbitrary keyword arguments (unused).
        """
        self._fit = None

    def fit(self, x: list, y: list, model: str = "linear") -> Any:
        """Fit the given data according to the given model.

        For a linear model: y(x) = slope * x + intersect

        Args:
            x: Input x data points.
            y: Input y data points.
            model: Model name. Defaults to "linear".

        Raises:
            UnknownException: If constructing the Fit object fails.

        Returns:
            Fit: Fit object containing the fitted parameters and diagnostics.
        """
        try:
            self._fit = Fit(x=x, y=y, model=model)
        except Exception as exc:
            raise UnknownException(exc) from exc

        return self._fit

    def slope(self) -> Union[float, None]:
        """Return the slope of a linear fit y(x) = slope * x + intersect.

        Raises:
            NotDefined: If the fit has not been defined.

        Returns:
            float | None: The slope value if available, otherwise None.
        """
        if not self._fit:
            raise NotDefined("Fit has not been defined")

        return self._fit.slope()

    def intersect(self) -> Union[float, None]:
        """Return the intersect of a linear fit y(x) = slope * x + intersect.

        Raises:
            NotDefined: If the fit has not been defined.

        Returns:
            float | None: The intersect value if available, otherwise None.
        """
        if not self._fit:
            raise NotDefined("Fit has not been defined")

        return self._fit.intersect()

    def chi2(self) -> Union[float, None]:
        """Return the chi2 of the fit.

        Raises:
            NotDefined: If the fit has not been defined.

        Returns:
            float | None: The chi2 value if available, otherwise None.
        """
        if not self._fit:
            raise NotDefined("Fit has not been defined")

        return self._fit.chi2()

    def get_table(self, filename: str, header: str = "", separator: str = "\t", convert_to_float: bool = True) -> dict:
        """Return a table read from a file.

        Args:
            filename: Full path to the input file.
            header: Header string. Defaults to "".
            separator: Column separator character. Defaults to tab.
            convert_to_float: If True, all values are converted to floats. Defaults to True.

        Returns:
            dict: Table data keyed by column name.
        """
        return get_table_from_file(
            filename,
            header=header,
            separator=separator,
            convert_to_float=convert_to_float,
        )

    def get_fitted_data(
        self, filename: str, x_name: str = "Time", y_name: str = "pss+swap", precision: int = 2, tails: bool = True
    ) -> dict:
        """Return a dictionary of analytics fit results for the given memory monitor output file.

        Currently fits PSS+Swap vs time; the slope of this fit measures memory leaks.

        Args:
            filename: Full path to the memory monitor output file.
            x_name: Column name to use as the x-axis. Defaults to "Time".
            y_name: Column name to use as the y-axis (may contain a '+' to sum two columns).
                Defaults to "pss+swap".
            precision: Decimal precision for the rounded output values. Defaults to 2.
            tails: If True, retain the first and last data points (tails). For large samples
                (>=100 points) the iterative method is used regardless. Defaults to True.

        Returns:
            dict: A dictionary with keys ``"slope"``, ``"chi2"``, and ``"intersect"``, each
                holding a rounded string representation of the corresponding fit parameter,
                or an empty string if the fit could not be performed.
        """
        slope = ""
        intersect = ""
        _chi2 = ""
        table = self.get_table(filename)

        if table:
            # extract data to be fitted
            x, y = self.extract_from_table(table, x_name, y_name)

            # remove tails if desired
            # this is useful e.g. for memory monitor data where the first and last values
            # represent allocation and de-allocation, ie not interesting

            itmet = False
            if len(x) >= 100:
                logger.debug(
                    "tails will not be removed for large data sample - iterative method will be used instead"
                )
                tails = True
                itmet = True

            if not tails and len(x) > 7 and len(y) > 7:
                logger.debug("removing tails from data to be fitted")
                x = x[5:]
                x = x[:-2]
                y = y[5:]
                y = y[:-2]

            if not (len(x) > 7 and len(y) > 7) and len(x) == len(y):
                logger.warning(
                    "wrong length of table data, x=%s, y=%s (must be same and length>=4)",
                    str(x),
                    str(y),
                )
            else:
                logger.info("fitting %s vs %s", y_name, x_name)

                if itmet:
                    norg = len(x)
                    fit = self.fit(x, y)
                    _chi2_org = fit.chi2()

                    # only attempt iterative trimming if the initial chi2 is valid and non-zero
                    # (bug fix: guard against None or zero chi2 causing ZeroDivisionError in find_limit)
                    if _chi2_org is not None and _chi2_org != 0:
                        # determine the removable right region ("right side limit")
                        _x = x
                        _y = y
                        right_limit = self.find_limit(_x, _y, _chi2_org, norg, edge="right")

                        # determine the removable left region ("left side limit")
                        _x = x
                        _y = y
                        left_limit = self.find_limit(_x, _y, _chi2_org, norg, edge="left")

                        # final fit adjusted for removable regions
                        x = x[left_limit:right_limit]
                        y = y[left_limit:right_limit]

                try:
                    fit = self.fit(x, y)
                    _slope = self.slope()
                except Exception as exc:
                    logger.warning(f"failed to fit data, x={x}, y={y}: {exc}")
                else:
                    # bug fix: use explicit None check so a legitimate zero slope is not discarded
                    if _slope is not None:
                        slope = float_to_rounded_string(fit.slope(), precision=precision)
                        intersect = float_to_rounded_string(fit.intersect(), precision=precision)
                        _chi2 = float_to_rounded_string(fit.chi2(), precision=precision)
                        if slope != "":
                            logger.info(
                                f"current memory leak: {slope} B/s (using {len(x)} data points, chi2={_chi2})"
                            )

        return {"slope": slope, "chi2": _chi2, "intersect": intersect}

    def find_limit(
        self,
        _x: list,
        _y: list,
        _chi2_org: float,
        norg: int,
        change_limit: float = 0.25,
        edge: str = "right",
        steps: int = 5,
    ) -> int:
        """Use an iterative approach to find the trimming limit on one edge of the distribution.

        Iteratively removes ``steps`` data points from the specified edge and re-fits,
        stopping when the relative improvement in chi2 falls below ``change_limit``.
        The search continues only while at least two thirds of the original data remain.

        Args:
            _x: x data points.
            _y: y data points (same length as ``_x``).
            _chi2_org: Chi2 value of the initial fit before trimming.
            norg: Original number of data points before any trimming.
            change_limit: Minimum relative chi2 improvement required to continue trimming.
                Defaults to 0.25.
            edge: Which edge to trim; either ``"right"`` or ``"left"``. Defaults to ``"right"``.
            steps: Number of points removed per iteration. Defaults to 5.

        Returns:
            int: Index into the original arrays representing the trim boundary.
                For the right edge this is the last index to keep; for the left edge
                it is the first index to keep.
        """
        _chi2_prev = _chi2_org
        found = False
        iterations = 0
        while len(_x) > 2 * norg / 3:
            iterations += 1
            if edge == "right":
                _x = _x[:-steps]
                _y = _y[:-steps]
            else:  # left edge
                _x = _x[steps:]
                _y = _y[steps:]
            try:
                fit = self.fit(_x, _y)
            except Exception as exc:
                logger.warning(f"caught exception: {exc}")
                break

            _chi2 = fit.chi2()

            # bug fix: guard against None or zero chi2_prev to prevent ZeroDivisionError or TypeError
            if _chi2 is None or _chi2_prev is None or _chi2_prev == 0:
                logger.warning(f"invalid chi2 value(s): chi2={_chi2}, chi2_prev={_chi2_prev} - stopping iteration")
                break

            change = (_chi2_prev - _chi2) / _chi2_prev
            logger.info(f"current chi2={_chi2} (change={change * 100} %)")
            if change < change_limit:
                found = True
                break

            _chi2_prev = _chi2

        if edge == "right":
            if not found:
                limit = norg - 1
                logger.warning("right removable region not found")
            else:
                limit = len(_x) - 1
                logger.info(f"right removable region: {limit}")
        elif not found:
            limit = 0
            logger.info("left removable region not found")
        else:
            # bug fix: was hardcoded as iterations * 10; correct formula is iterations * steps
            limit = iterations * steps
            logger.info(f"left removable region: {limit}")

        return limit

    def extract_from_table(self, table: dict, x_name: str, y_name: str) -> tuple:
        """Extract x and y data series from a table dictionary.

        If ``y_name`` contains a ``'+'`` character, the two named columns are summed
        element-wise to produce the y series.

        Args:
            table: Dictionary mapping column names to lists of values.
            x_name: Key of the column to use as the x series.
            y_name: Key of the column to use as the y series. May be of the form
                ``"col1+col2"`` to sum two columns.

        Returns:
            tuple: A two-element tuple ``(x, y)`` where each element is a list of values.
                Both lists are empty on failure.
        """
        x = table.get(x_name, [])
        if "+" not in y_name:
            y = table.get(y_name, [])
        else:
            try:
                y1_name = y_name.split("+")[0]
                y2_name = y_name.split("+")[1]
                y1_value = table.get(y1_name, [])
                y2_value = table.get(y2_name, [])
            except Exception as error:
                logger.warning("exception caught: %s", error)
                x = []
                y = []
            else:
                # create new list with added values (1,2,3) + (4,5,6) = (5,7,9)
                y = [x0 + y0 for x0, y0 in zip(y1_value, y2_value)]

        return x, y


class Fit():
    """Low-level fitting class."""

    _model = "linear"   # fitting model
    _x = None           # x values (original, uncentered)
    _y = None           # y values
    _x_offset = None    # x centering offset (mean of x) to prevent catastrophic cancellation
    _xm = None          # x mean of centered data (always 0.0 after centering)
    _ym = None          # y mean
    _ss = None          # sum of square deviations
    _ss2 = None         # sum of deviations
    _slope = None       # slope
    _intersect = None   # intersect (y value at center of x range, i.e. at x = _x_offset)
    _chi2 = None        # chi2

    def __init__(self, **kwargs: Any) -> None:
        """Initialize and perform the fit.

        Args:
            **kwargs: Keyword arguments. Recognized keys are:

                - ``model`` (str): Fitting model name. Defaults to ``"linear"``.
                - ``x`` (list): x data points.
                - ``y`` (list): y data points.

        Raises:
            NotDefined: If ``x`` or ``y`` are not provided or are empty.
            NotSameLength: If ``x`` and ``y`` have different lengths.
            NotImplementedError: If the requested model is not implemented.
        """
        # extract parameters
        self._model = kwargs.get("model", "linear")
        self._x = kwargs.get("x", None)
        self._y = kwargs.get("y", None)

        if not self._x or not self._y:
            raise NotDefined("input data not defined")

        if len(self._x) != len(self._y):
            raise NotSameLength("input data (lists) have different lengths")

        logger.debug(f'model: {self._model}, x: {self._x}, y: {self._y}')
        # base calculations
        if self._model == "linear":
            # bug fix: center x values around their mean to prevent catastrophic cancellation
            # in the intersect calculation when x values are large (e.g. Unix timestamps).
            # sum_square_dev and sum_dev both subtract the mean internally, so the slope
            # is unaffected. Setting _xm=0 makes intersect = y_mean (the y value at the
            # center of the x range), which is numerically stable.
            self._x_offset = mean(self._x)
            self._ss = sum_square_dev(self._x)
            logger.info("sum of square deviations: %s", self._ss)
            self._ss2 = sum_dev(self._x, self._y)
            logger.info("sum of deviations: %s", self._ss2)
            self.set_slope()
            self._xm = 0.0  # mean of (x - x_offset) is zero by construction
            logger.info("mean x (centered): %s", self._xm)
            self._ym = mean(self._y)
            logger.info("mean y: %s", self._ym)
            self.set_intersect()
            logger.info("intersect: %s", self._intersect)
            self.set_chi2()
            logger.info("chi2: %s", self._chi2)
        else:
            logger.warning("'%s' model is not implemented", self._model)
            raise NotImplementedError()

    def fit(self) -> "Fit":
        """Return the fitting object itself.

        Returns:
            Fit: This Fit instance.
        """
        return self

    def value(self, t: float) -> float:
        """Return the fitted y value at a given x position.

        Evaluates y(x=t) = slope * (t - x_offset) + intersect for the linear model,
        where ``x_offset`` is the mean of the original x data used to center the fit
        and ``intersect`` is the y value at the center of the x range.

        Args:
            t: The x position (in original, uncentered coordinates) at which to evaluate
                the fit.

        Returns:
            float: The fitted y value at x equal to t.
        """
        return self._slope * (t - self._x_offset) + self._intersect

    def set_chi2(self) -> None:
        """Calculate and store the chi2 value of the current fit.

        Computes expected y values from the linear model for each x point and
        compares them to the observed y values using the chi2 function.
        Sets ``_chi2`` to None if slope or intersect are undefined, or if either
        the observed or expected lists are empty.
        """
        # bug fix: guard against None slope/intersect to prevent TypeError in value()
        if self._slope is None or self._intersect is None:
            self._chi2 = None
            return

        y_observed = self._y
        y_expected = []
        for x in self._x:
            y_expected.append(self.value(x))
        if y_observed and y_observed != [] and y_expected and y_expected != []:
            self._chi2 = chi2(y_observed, y_expected)
        else:
            self._chi2 = None

    def chi2(self) -> Union[float, None]:
        """Return the chi2 value of the fit.

        Returns:
            float | None: The chi2 value, or None if it could not be calculated.
        """
        return self._chi2

    def set_slope(self) -> None:
        """Calculate and store the slope of the linear fit.

        Sets ``_slope`` to None if the required sums are zero or undefined.
        """
        # bug fix: use explicit None checks instead of truthiness so that a
        # legitimate zero slope (flat data) is stored as 0.0 rather than None
        if self._ss2 is not None and self._ss is not None and self._ss != 0:
            self._slope = self._ss2 / float(self._ss)
        else:
            self._slope = None

    def slope(self) -> Union[float, None]:
        """Return the slope of the linear fit.

        Returns:
            float | None: The slope value, or None if it could not be calculated.
        """
        return self._slope

    def set_intersect(self) -> None:
        """Calculate and store the intersect of the linear fit.

        Because x is centered around its mean before fitting, ``_xm`` is 0 and
        the intersect simplifies to ``y_mean``, i.e. the fitted y value at the
        center of the x range. This avoids catastrophic cancellation that would
        occur with the traditional formula when x values are large (e.g. Unix
        timestamps).

        Sets ``_intersect`` to None if any of the required mean or slope values
        are undefined.
        """
        if self._ym is not None and self._slope is not None and self._xm is not None:
            self._intersect = self._ym - self._slope * self._xm
            logger.debug(f"{self._ym} - {self._slope} * {self._xm} = {self._intersect}")
        else:
            self._intersect = None
            logger.info("could not calculate intersect")

    def intersect(self) -> Union[float, None]:
        """Return the intersect of the linear fit.

        The intersect is the fitted y value at the center of the x range
        (i.e. at ``x = x_offset``), not at ``x = 0``.

        Returns:
            float | None: The intersect value, or None if it could not be calculated.
        """
        return self._intersect
