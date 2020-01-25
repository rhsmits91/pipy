import datetime
import uuid

import ipywidgets as ipy
from IPython.display import display
from functools import partial
from traitlets import HasTraits, Any, Bool, Dict, List, Int, Unicode, validate

from pipy.parameters import Iterable, MultiSelect, Option
from pipy.widgets import FilterAndSelect


interactive_widget_registry = {}
widget_registry = {}
debugger = ipy.Output()


def _get_label(_, key):
    return ipy.HTML("<b>{}:</b>".format(key), layout=ipy.Layout(min_width="90px"))


class InteractiveTraitlet(HasTraits):
    label_position = ipy.VBox

    def __init__(self, obj, *args, **kwargs):
        self.uuid = str(uuid.uuid4())
        self.obj = obj
        interactive_widget_registry[self.uuid] = self
        super(InteractiveTraitlet, self).__init__(*args, **kwargs)

    @property
    def widget(self):
        return widget_registry.get(self.uuid, self.render())

    def refresh(self):
        self.render()

    def render(self):
        widget = self._render()
        widget_registry[self.uuid] = widget
        return widget

    def _render(self):
        return NotImplementedError

    def _ipython_display_(self):
        display(self._render())

    def _update(self, change):
        pass


class _Bool(InteractiveTraitlet):
    label_position = ipy.HBox
    obj = Bool()

    def _render(self):
        return ipy.Checkbox(value=self.obj)


class _Int(InteractiveTraitlet):
    obj = Int()

    def _render(self):
        return ipy.IntText(value=self.obj)


class _Str(InteractiveTraitlet):
    obj = Unicode()

    def _render(self):
        if "\n" in self.obj:
            widget = ipy.Textarea(value=self.obj)
            widget.layout.width = "640px"
            widget.layout.height = "180px"
        else:
            widget = ipy.Text(value=self.obj)
        return widget


class _Date(InteractiveTraitlet):
    obj = Any()

    @validate("obj")
    def _validate_date(self, proposal):
        if not isinstance(proposal["value"], datetime.date):
            raise ValueError(
                "`_Date.obj` should be a `datetime.date` but a value of type "
                "{} was passed".format(type(proposal))
            )
        return proposal["value"]

    def _render(self):
        return ipy.DatePicker(value=self.obj)


class _List(InteractiveTraitlet):
    obj = List()

    def _render(self):
        return ipy.Dropdown(options=self.obj)


class _MultiSelect(InteractiveTraitlet):
    obj = Any()

    def _render(self):
        widget = FilterAndSelect(value=self.obj.value, options=self.obj.options)
        widget.selected.observe(self._update, names=["options"])
        return widget

    def _update(self, change):
        self.obj.value = list(change["new"])


class _Option(InteractiveTraitlet):
    obj = Any()

    def _render(self):
        widget = ipy.Dropdown(value=self.obj.value, options=self.obj.options)
        widget.observe(self._update, names=["value"])
        return widget

    def _update(self, change):
        if isinstance(change["new"], Option):
            return
        if change["new"] not in self.obj.options:
            raise ValueError("Value must be on of {}.".format(self.obj.options))
        self.obj.value = change["new"]


class _Iterable(InteractiveTraitlet):
    obj = Any()

    def _render(self):
        widget = ipy.Text(value=str(self.obj.value))
        # widget.observe(self._update, names=['value'])
        return widget

    def _update(self, change):
        if isinstance(change["new"], Iterable):
            return
        if change["new"] not in self.obj.options:
            raise ValueError("Value must be on of {}.".format(self.obj.options))
        self.obj.value = change["new"]


class InteractiveDict(InteractiveTraitlet):
    obj = Dict()
    _registry = {}
    _iregistry = {}
    _renderers = {
        MultiSelect: _MultiSelect,
        Option: _Option,
        Iterable: _Iterable,
        bool: _Bool,
        datetime.date: _Date,
        int: _Int,
        list: _List,
        str: _Str,
    }
    _key_renderer = _get_label

    def _update(self, change, key):
        self._iregistry[key]._update(change)
        self.obj[key] = change["new"]

    def _get_widgets(self):
        for key, value in self.obj.items():
            label = self._key_renderer(key)
            renderer_cls = self._renderers.get(type(value), value)

            try:
                renderer = renderer_cls(obj=value)
            except TypeError:
                pass
            else:
                self._iregistry[key] = renderer
                widget = renderer.render()
                widget.observe(partial(self._update, key=key), names=["value"])
                self._registry[key] = widget
                yield renderer_cls.label_position([label, widget])

    def _render(self):
        widgets = tuple(self._get_widgets())
        if self.uuid in self._registry:
            widget = self._registry[self.uuid]
            widget.children = widgets
        else:
            widget = ipy.VBox(widgets)
            self._registry[self.uuid] = widget
        return widget
