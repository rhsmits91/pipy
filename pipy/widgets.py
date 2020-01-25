from operator import itemgetter

import ipywidgets as ipy


class FilterAndSelect(ipy.VBox):
    def __init__(self, **kwargs):
        """Public constructor"""
        self.options = kwargs.pop("options", [])
        selected = kwargs.pop("value", [])
        unselected = [o for o in self.options if o not in selected]
        selected = ipy.SelectMultiple(
            options=selected, layout=ipy.Layout(height="240px")
        )
        unselected = ipy.SelectMultiple(
            options=unselected, layout=ipy.Layout(height="240px")
        )
        super(FilterAndSelect, self).__init__(**kwargs)

        select = ipy.Button(description=">", layout=ipy.Layout(width="41px"))
        unselect = ipy.Button(description="<", layout=ipy.Layout(width="41px"))
        select_filtered = ipy.Button(description=">>", layout=ipy.Layout(width="41px"))
        unselect_filtered = ipy.Button(
            description="<<", layout=ipy.Layout(width="41px")
        )
        selectors = ipy.HBox(
            [
                ipy.Box(layout=ipy.Layout(width="45px")),
                unselected,
                ipy.VBox([select, select_filtered, unselect, unselect_filtered]),
                selected,
            ]
        )
        unselected_filter = ipy.Text(continuous_update=True)
        selected_filter = ipy.Text(continuous_update=True)
        filters = ipy.HBox(
            [
                ipy.HTML(
                    '<i class="fa fa-filter center label-icon" style="min-width=32px">'
                ),
                unselected_filter,
                ipy.HTML(
                    '<i class="fa fa-filter center label-icon" style="min-width=32px">'
                ),
                selected_filter,
            ]
        )

        def filter_unselected(change):
            unselected.options = [
                o
                for o in self.options
                if o not in selected.options and change["new"] in o
            ]

        def filter_selected(change):
            selected.options = [
                o
                for o in self.options
                if o not in unselected.options and change["new"] in o
            ]

        def add_selected(_):
            options_to_add = itemgetter(*unselected.index)(unselected.options)
            selected.options = [
                o for o in self.options if o in selected.options or o in options_to_add
            ]
            unselected.options = [
                o for o in unselected.options if o not in options_to_add
            ]

        def add_filtered(_):
            selected.options = [
                o
                for o in self.options
                if o in selected.options or o in unselected.options
            ]
            unselected.options = []

        def remove_selected(_):
            options_to_remove = itemgetter(*selected.index)(selected.options)
            unselected.options = [
                o
                for o in self.options
                if o in unselected.options or o in options_to_remove
            ]
            selected.options = [
                o for o in selected.options if o not in options_to_remove
            ]

        def remove_filtered(_):
            unselected.options = [
                o
                for o in self.options
                if o in unselected.options or o in selected.options
            ]
            selected.options = []

        select.on_click(add_selected)
        select_filtered.on_click(add_filtered)
        selected_filter.observe(filter_selected, names="value")
        unselect.on_click(remove_selected)
        unselect_filtered.on_click(remove_filtered)
        unselected_filter.observe(filter_unselected, names="value")
        self.children = (selectors, filters)
        self.selected = selected

    def __iter__(self):
        yield from self.selected.options
