def recursive_update(d, keys, value):
    if isinstance(keys, list):
        key_list = keys
    else:
        key_list = keys.split('.')
    #print('1)', key_list)
    if len(key_list) == 1:
        if key_list[0] in d and (type(d) is dict and type(d[key_list[0]]) is dict):
            d[key_list[0]].update(value)
        else:
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


def recursive_exists(d, key):
    key_list = key.split(".")
    current_d = d
    for k in key_list[:-1]:
        if type(current_d) is not dict:
            return False
        else:
            current_d = current_d[k]
    return key_list[-1] in current_d


# http://stackoverflow.com/questions/2130016/splitting-a-list-of-arbitrary-size-into-only-roughly-n-equal-parts
def split_list(a, n):
    k, m = int(len(a) / n), len(a) % n
    return {i: a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)}
