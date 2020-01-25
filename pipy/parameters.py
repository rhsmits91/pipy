class PandasParam:
    def __init__(self, value, options=None):
        self._value = value
        self.options = options

    @property
    def value(self):
        return self._value() if callable(self._value) else self._value

    def __repr__(self):
        return str("{}({}, {})".format(type(self).__name__, self.value, self.options))

    def __call__(self, *args, **kwargs):
        return self.value

    def update(self, *args, **kwargs):
        raise NotImplemented


class Option(PandasParam):
    def __iter__(self):
        yield self.value

    def update(self, value, options=None):
        self._value = value
        self.options = options or [value]
        return self


class Iterable(PandasParam):
    def __init__(self, value, dtype):
        super(Iterable, self).__init__(value)
        self.dtype = dtype

    def __iter__(self):
        yield from self.value

    def update(self, value):
        self._value = value
        return self


class MultiSelect(Iterable):
    def update(self, value, options=None):
        self._value = value
        self.options = options or value
        return self
