import pandas as pd

from pipy.parameters import Iterable, MultiSelect, Option
from pipy.pipeline import Step
from pipy.pipeline.utils import combine_series


class Transform(Step):
    _columns = {"in": MultiSelect([], [])}


class DayOfWeek(Transform):
    _columns = {"in": Option("", [])}

    def transform(self, df: pd.DataFrame):
        s = df[self.columns["in"].value]
        s = s.astype("datetime64[ns]")
        s = s.dt.dayofweek
        s.name = self.get_columns_out()[0]
        return pd.concat([df, s], axis=1)


class Normalise(Transform):
    def fit(self, df: pd.DataFrame) -> None:
        means = df[self.columns["in"]].mean()
        self.coeffs["means"] = combine_series(
            self.coeffs.get("means", pd.Series), means
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df_ = df[self.columns["in"]] / self.coeffs["means"]
        df_.columns = self.get_columns_out()
        return pd.concat([df, df_], axis=1)


class MovingAverage(Transform):
    _params = {"periods": Iterable([], int), "pandas_kwargs": {}}

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        dfs = []
        for p in self.params["periods"]:
            dfs.append(
                df[self.columns["in"]]
                .rolling(window=p, **self.params["pandas_kwargs"])
                .mean()
            )

        df_ = pd.concat(dfs, axis=1)
        df_.columns = self.get_columns_out()
        return pd.concat([df, df_], axis=1)
