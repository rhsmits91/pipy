from textwrap import dedent

import pandas as pd

try:
    from psycopg2.extensions import connection
except:
    PSYCOPG2_NOT_AVAILABLE = True
else:
    PSYCOPG2_NOT_AVAILABLE = False

from pipy.pipeline import Step


class Extract(Step):
    def get_columns_in(self):
        return []

    def extract(self, df: pd.DataFrame = pd.DataFrame()):
        return df

    def transform(self, df: pd.DataFrame = pd.DataFrame()):
        return self.extract(df)


class CSV(Extract):
    _params = {"path": "", "pandas_kwargs": {}}

    def extract(self, df: pd.DataFrame = pd.DataFrame()):
        options = {"header": 0}
        options.update(self.params["pandas_kwargs"])
        return pd.read_csv(self.params["path"], **options)

    def get_columns_out(self):
        options = {"header": 0}
        options.update(self.params["pandas_kwargs"])
        options.update(nrows=0)
        return pd.read_csv(self.params["path"], **options).columns.tolist()


class SQL(Extract):
    _params = {
        "dsn": "",
        "params": {},
        "query": dedent(
            """\
            SELECT * FROM table_name
            LIMIT 10
            """
        ),
    }

    def extract(self, df: pd.DataFrame = pd.DataFrame()):
        with connection(self.params["dsn"]) as cxn:
            _df = pd.read_sql(self.params["query"], cxn, params=self.params["params"])
            return pd.concat([df, _df], axis=1)

    def get_columns_out(self):
        with connection(self.params["dsn"]) as cxn:
            return pd.read_sql(
                self.params["query"] + " LIMIT 0", cxn, params=self.params["params"]
            ).columns.tolist()
