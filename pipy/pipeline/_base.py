import logging
import sys
import uuid
from copy import deepcopy
from joblib import hash as hashy

import graphviz
import ipywidgets as ipy
import networkx as nx
import pandas as pd
from IPython.display import display

from pipy.interactive import InteractiveDict
from pipy.parameters import Iterable, PandasParam


logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def get_label(s):
    return ipy.HTML("<b>{}</b>".format(s))


class Step:
    _columns = {}
    _params = {}
    coeffs = {}

    @property
    def name(self):
        return type(self).__name__

    def __init__(self, params: dict = None, columns: dict = None):
        self.uuid = str(uuid.uuid4())
        self.columns = self._init_columns(columns or {})
        self.params = self._init_params(params or {})

    def _init_columns(self, columns):
        return {k: deepcopy(v).update(columns.get(k)) for k, v in self._columns.items()}

    def _init_params(self, params):
        return {
            k: (
                v.update(params.get(k))
                if isinstance(v, PandasParam)
                else params.get(k, v)
            )
            for k, v in self._params.items()
        }

    def _ipython_display_(self):
        display(self.render())

    def _iter(self):
        iter_params = {k for k, v in self.params.items() if isinstance(v, Iterable)}
        if not iter_params:
            for c in self.columns.get("in", []):
                yield self.params, c
        else:
            for k in iter_params:
                for p in self.params[k]:
                    for c in self.columns["in"]:
                        yield {k: p}, c

    def get_columns_in(self):
        return [c for k, l in self.columns.items() for c in l if k != "out"]

    def get_columns_out(self):
        def get_param_str(p):
            param_str = ",".join("{}={}".format(k, v) for k, v in p.items())
            return "({})".format(param_str) if param_str else param_str

        return [
            "{}|{}{}".format(c, self.name, get_param_str(p)) for p, c in self._iter()
        ]

    def get_dag(self):
        dag = nx.DiGraph()
        for (p, i), o in zip(self._iter(), self.get_columns_out()):
            dag.add_node(o, params=p)
            dag.add_edge(i, o)
        return dag

    def update_available_columns(self, columns):
        for key, c in self.columns.items():
            if key == "out":
                continue
            c.options = columns

    def render(self):
        widgets = []
        hspace = ipy.Box([], layout=ipy.Layout(min_width="20px"))
        column_widgets = InteractiveDict(self.columns).render()
        if column_widgets.children:
            widgets.append(get_label("Columns:"))
            widgets.append(ipy.HBox([hspace, column_widgets]))
        param_widgets = InteractiveDict(self.params).render()
        if param_widgets.children:
            widgets.append(get_label("Parameters:"))
            widgets.append(ipy.HBox([hspace, param_widgets]))
        return ipy.VBox(widgets)

    def fit(self, df: pd.DataFrame) -> None:
        pass

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.fit(df)
        return self.transform(df)


class Pipeline(Step):
    _params = {"steps": []}

    def __init__(self, params: dict = None, columns: dict = None):
        super(Pipeline, self).__init__(params, columns)
        self.dag = self.get_dag()
        self.df = pd.DataFrame()
        self.update_available_columns()

    def update_available_columns(self, columns: list = None):
        all_columns = []
        for step in self.params["steps"]:
            step.update_available_columns(all_columns.copy())
            all_columns += step.get_columns_out()

    def get_dag(self):
        dag = nx.DiGraph()
        for step in self.params["steps"]:
            dag.update(step.get_dag())
        return dag

    def display_dag(self):
        dag = graphviz.Digraph(
            graph_attr={"fixedsize": "false", "outputorder": "edgesfirst"},
            node_attr={
                "height": "0.4",
                "fontsize": "11",
                "style": "filled",
                "color": "white",
            },
            edge_attr={"arrowsize": "0.6"},
        )
        for step in self.params["steps"]:
            columns_in = step.get_columns_in()
            columns_out = step.get_columns_out()
            dag.node(step.name, shape="box", color="lightblue")
            with dag.subgraph() as s:
                for c in columns_out:
                    s.attr(rank="same")
                    s.node(c, shape="box", height="0.2")
            dag.edges([(c, step.name) for c in columns_in])
            dag.edges([(step.name, c) for c in columns_out])
        display(dag)

    def _ipython_display_(self):
        steps = [s.render() for s in self.params["steps"]]
        widget = ipy.Accordion(steps)
        widget.selected_index = None
        for n, s in enumerate(self.params["steps"]):
            widget.set_title(n, s.name)

        output = ipy.Output(
            layout=ipy.Layout(overflow="auto", _webkit_overflow_y="auto")
        )
        with output:
            self.display_dag()

        tabs = ipy.Tab([widget, output])
        tabs.set_title(0, "Steps")
        tabs.set_title(1, "Blueprint")
        display(tabs)

    def fit(self, df: pd.DataFrame) -> None:
        for s in self.params["steps"]:
            s.fit(df)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        for s in self.params["steps"]:
            self.df = s.transform(self.df)
        return self.df

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        for s in self.params["steps"]:
            s.fit(self.df)
            self.df = s.transform(self.df)
        return self.df

    def run(self):
        self.df = self.fit_transform(self.df)
        return self.df


class Skippy(Pipeline):
    coeffs = {"hashes": pd.Series()}

    def get_dependents(self, column):
        for dependent in self.dag.successors(column):
            yield dependent
            yield from self.get_dependents(dependent)

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        old_hashes = self.coeffs["hashes"]
        if old_hashes.empty:
            df_ = df
        else:
            now_hashes = df.apply(hashy, axis=0)
            columns = list(self.dag.nodes())
            now_hashes = now_hashes.reindex(columns)
            old_hashes = old_hashes.reindex(columns)
            if now_hashes.equals(old_hashes):
                logger.info("No changes detected - skipping.")
                return df

            changed_columns = now_hashes.index[now_hashes != old_hashes].tolist()
            if len(changed_columns) < now_hashes.shape[0]:
                logger.info(
                    "Changes detected - rerunning pipeline for {} only.".format(
                        changed_columns
                    )
                )
                dependents = {
                    d for c in changed_columns for d in self.get_dependents(c)
                }
                df_ = df[dependents]
            else:
                df_ = df

        df_ = super(Skippy, self).fit_transform(df_)
        new_hashes = df_.apply(hashy, axis=0)
        self.coeffs["hashes"] = old_hashes.combine_first(new_hashes)
        self.df.update(df_)
        return self.df
