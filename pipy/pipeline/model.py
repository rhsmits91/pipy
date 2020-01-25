import numpy as np
import pandas as pd

from pipy.pipeline import Step
from pipy.parameters import Option, MultiSelect


class Model(Step):
    _columns = {"target": Option(None, []), "features": MultiSelect([], [])}

    def get_columns_out(self):
        return ["{}|{}".format(self.columns["target"].value, self.name)]

    def update_available_columns(self, columns):
        target = self.columns["target"].value
        self.columns["target"].options = columns
        self.columns["features"].options = [c for c in columns if c != target]


class SkLearnModelWrapper(Model):
    _params = {"sklearn_model": None}

    @property
    def name(self):
        return type(self.params["sklearn_model"]).__name__

    def _init_params(self, params):
        params = super(SkLearnModelWrapper, self)._init_params(params)
        return dict(**params["sklearn_model"].get_params(), **params)

    def fit(self, df: pd.DataFrame) -> None:
        model = self.params["sklearn_model"]
        features = self.columns["features"].value
        model.fit(df[features], df[self.columns["target"].value])
        self.coeffs["coeffs"] = pd.Series(
            np.append(model.coef_, model.intercept_), index=features + ["Intercept"]
        )

    def transform(self, df: pd.DataFrame):
        features = self.columns["features"].value
        df_ = df[features]
        coeffs = self.coeffs["coeffs"]
        df_ = (df_ * coeffs[features]).sum(axis=1).to_frame()
        if "Intercept" in coeffs:
            df_ += coeffs["Intercept"]
        df_.columns = self.get_columns_out()
        return pd.concat([df, df_], axis=1)
