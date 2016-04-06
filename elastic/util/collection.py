def recursive_update(d, keys, value):
    if isinstance(keys, list):
        key_list = keys
    else:
        key_list = keys.split('.')
    if len(key_list) == 1:
        d[key_list[0]] = value
        return d
    else:
        if key_list[0] not in d.keys():
            d[key_list[0]] = {}
        sub_d = d[key_list[0]]
        if not isinstance(sub_d, dict):
            sub_d = {}
        new_sub_d = recursive_update(sub_d, key_list[1:], value)
        d[key_list[0]] = new_sub_d
        return d


def recursive_lookup(d, key, default_value=None):
    key_list = key.split(".")
    try:
        current_d = d
        for k in key_list:
            current_d = current_d[k]
        return current_d
    except Exception as e:
        if default_value is not None:
            return default_value
        raise e