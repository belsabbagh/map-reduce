from src import Mapper, Reducer, Grouper, MapWithDuplicateKeys

# word count example


def map_fn(text):
    _map = MapWithDuplicateKeys()
    for word in text.split():
        _map(**{word: 1})
    return _map._map


def group_fn(maps):
    merged_map = []
    for m in maps:
        merged_map.extend(m)
    keys = set([i[0] for i in merged_map])
    grouped_map = []
    for key in keys:
        shard = []
        for i in merged_map:
            if i[0] == key:
                shard.append(i)
        grouped_map.append(shard)
    return grouped_map


def reduce_fn(m: list[tuple]):
    return (m[0][0], sum([i[1] for i in m]))


if __name__ == "__main__":
    text = "hello world\nhey you\nhello world"
    shards = text.split("\n")
    maps = [Mapper(map_fn)(i) for i in shards]
    print(maps)
    new_maps = Grouper(group_fn)(maps)
    print(new_maps)
    reductions = [Reducer(reduce_fn)(i) for i in new_maps]
    print(reductions)
