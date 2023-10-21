from src import MapReduceBuilder, MapWithDuplicateKeys, KeyGrouper, Reducer, Mapper


def map_fn(text):
    _map = MapWithDuplicateKeys()
    for word in text.strip().replace("\n", "").lower().split():
        _map(**{word: 1})
    return _map._map


def reduce_fn(m: list[tuple]):
    return (m[0][0], sum([i[1] for i in m]))


if __name__ == "__main__":
    with open("test_data.txt", "r") as f:
        shards = f.readlines()
    mpr = MapReduceBuilder(Mapper(map_fn), KeyGrouper(), Reducer(reduce_fn))
    res = mpr(shards)
    sorted_res = sorted(res, key=lambda x: x[1], reverse=True)
    print(sorted_res)
