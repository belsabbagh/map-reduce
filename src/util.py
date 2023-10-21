def group_by_key(maps):
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
