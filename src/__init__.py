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


class MapReduceBuilder:
    def __init__(self, map_fn, group_fn, reduce_fn) -> None:
        self.map_fn = map_fn
        self.reduce_fn = reduce_fn
        self.group_fn = group_fn

    def __call__(self, data, *args, **kwargs):
        maps = [Mapper(self.map_fn)(i, *args, **kwargs) for i in data]
        new_maps = Grouper(self.group_fn)(maps, *args, **kwargs)
        return [Reducer(self.reduce_fn)(i, *args, **kwargs) for i in new_maps]


class MapWithDuplicateKeys:
    _map: list[tuple]

    def __init__(self) -> None:
        self._map = []

    def __call__(self, **kwargs):
        self._map.extend(kwargs.items())
