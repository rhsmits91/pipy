import pandas as pd

from pipy.columns import All
from pipy.pipeline import Step


class Load(Step):
    def get_columns_in(self):
        return All()

    def get_columns_out(self):
        return []

    def load(self, df: pd.DataFrame):
        return df

    def transform(self, df: pd.DataFrame):
        return self.load(df)


class CSV(Load):
    _params = {"path": "", "pandas_kwargs": {}}

    def load(self, df):
        df.to_csv(self.params["file"], **self.params["pandas_kwargs"])
        return df
