def combine_series(s1, s2):
    if s1.empty:
        return s2
    index = s1.index | s2.index
    combined = s1.reindex(index)
    combined.update(s2)
    return combined.astype(s1.dtype)
