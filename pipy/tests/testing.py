import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from pipy import pipeline


def get_dummy_dataset():
    df = pd.DataFrame(
        np.arange(20).reshape(10, 2), columns=["added_notional", "removed_notional"]
    )
    df["firm_id"] = ["A", "B"] * 5
    df["date"] = pd.date_range("2019-01-01", periods=5, freq="MS").repeat(2)
    df = df.sort_values(["firm_id", "date"])
    return df.reset_index(drop=True)


class DummyData(pipeline.extract.Extract):
    def extract(self, df: pd.DataFrame = pd.DataFrame()):
        return get_dummy_dataset()

    def get_columns_out(self):
        return ["added_notional", "removed_notional", "firm_id", "date"]


def get_etl_pipeline():
    extract = DummyData()
    weekday = pipeline.transform.DayOfWeek(columns={"in": "date"})
    ols = pipeline.model.SkLearnModelWrapper(
        columns={"target": "added_notional", "features": ["date|DayOfWeek"]},
        params={"sklearn_model": LinearRegression(fit_intercept=True, normalize=False)},
    )
    load = pipeline.load.CSV(params={"path": "demo.csv"})
    return pipeline.Pipeline({"steps": [extract, weekday, ols, load]})


def get_skippy_pipeline():
    extract = DummyData()
    mav = pipeline.transform.MovingAverage(
        columns={"in": ["added_notional", "removed_notional"]},
        params={"periods": [2, 3]},
    )
    std = pipeline.transform.Normalise(
        columns={"in": mav.get_columns_out},
    )
    return pipeline.Skippy({"steps": [extract, mav, std]})
