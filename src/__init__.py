from src.util import group_by_key


class Mapper:
    def __init__(self, map_fn) -> None:
        self.map_fn = map_fn

    def __call__(self, data, *args, **kwargs):
        return self.map_fn(data, *args, **kwargs)


class Reducer:
    def __init__(self, reduce_fn) -> None:
        self.reduce_fn = reduce_fn

    def __call__(self, maps, *args, **kwargs):
        return self.reduce_fn(maps, *args, **kwargs)


class Shuffler:
    def __init__(self, shuffle_fn) -> None:
        self.shuffle_fn = shuffle_fn

    def __call__(self, maps, *args, **kwargs):
        return self.shuffle_fn(maps, *args, **kwargs)


class Grouper(Shuffler):
    def __init__(self, group_fn) -> None:
        super().__init__(group_fn)


class KeyGrouper(Grouper):
    def __init__(self) -> None:
        super().__init__(group_by_key)


class MapReduceBuilder:
    def __init__(self, mapper, grouper, reducer) -> None:
        self.mapper = mapper
        self.grouper = grouper
        self.reducer = reducer

    def __call__(self, data, *args, **kwargs):
        mapped = [self.mapper(d, *args, **kwargs) for d in data]
        grouped = self.grouper(mapped, *args, **kwargs)
        reduced = [self.reducer(g, *args, **kwargs) for g in grouped]
        return reduced


class MapWithDuplicateKeys:
    _map: list[tuple]

    def __init__(self) -> None:
        self._map = []

    def __call__(self, **kwargs):
        self._map.extend(kwargs.items())
