import pandas as pd

from pipy.pipeline.utils import combine_series


def test_combine_series():
    s1 = pd.Series(dict(zip("AB", (1, 2))))
    s2 = pd.Series(dict(zip("BC", (20, 30))))
    s3 = combine_series(s1, s2)
    pd.testing.assert_series_equal(s3, pd.Series({"A": 1, "B": 20, "C": 30}))
