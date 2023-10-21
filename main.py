from src import MapReduceBuilder, MapWithDuplicateKeys


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
    mpr = MapReduceBuilder(map_fn, group_fn, reduce_fn)
    print(mpr(shards))
