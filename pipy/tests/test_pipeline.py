import pandas as pd
from mock import patch
from testfixtures import compare

from pipy.tests import testing
from pipy.pipeline.load import CSV


@patch.object(CSV, "load", lambda _, df: df)
def test_pipeline():
    pipe = testing.get_etl_pipeline()
    df_expected = pd.DataFrame(
        [
            [0, 1, "A", pd.to_datetime("2019-01-01"), 1, 9.750],
            [4, 5, "A", pd.to_datetime("2019-02-01"), 4, 7.875],
            [8, 9, "A", pd.to_datetime("2019-03-01"), 4, 7.875],
            [12, 13, "A", pd.to_datetime("2019-04-01"), 0, 10.375],
            [16, 17, "A", pd.to_datetime("2019-05-01"), 2, 9.125],
            [2, 3, "B", pd.to_datetime("2019-01-01"), 1, 9.750],
            [6, 7, "B", pd.to_datetime("2019-02-01"), 4, 7.875],
            [10, 11, "B", pd.to_datetime("2019-03-01"), 4, 7.875],
            [14, 15, "B", pd.to_datetime("2019-04-01"), 0, 10.375],
            [18, 19, "B", pd.to_datetime("2019-05-01"), 2, 9.125],
        ],
        columns=[
            "added_notional",
            "removed_notional",
            "firm_id",
            "date",
            "date|DayOfWeek",
            "added_notional|LinearRegression",
        ],
    )
    df = pipe.run()
    pd.testing.assert_frame_equal(df, df_expected)


def test_pipeline_dag():
    pipe = testing.get_etl_pipeline()
    pipe.dag


def test_skippy(caplog):
    pipe = testing.get_skippy_pipeline()
    pipe.run()
    compare(caplog.messages, expected=[])
    pipe.run()
    compare(caplog.messages, expected=["No changes detected - skipping."])
    caplog.clear()
    pipe.df["added_notional"] = 1
    pipe.run()
    compare(
        caplog.messages,
        expected=[
            "Changes detected - rerunning pipeline for ['added_notional'] only."
        ],
    )
    pipe.df = pd.DataFrame()
    pipe.run()
    # mav = pipe.params['steps'][1]
    # mav.params['periods'].value += [4]
    # pipe.run()
    # compare(
    #     caplog.messages,
    #     expected=['Changes detected - rerunning pipeline for 2 columns only.']
    # )
