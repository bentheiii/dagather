from typing import Mapping, TypeVar, Dict, Iterable, Collection, Any

K = TypeVar('K')
V = TypeVar('V')


def filter_dict(origin: Mapping[K, V], keys: Iterable[K]) -> Dict[K, V]:
    return {k: origin[k] for k in keys}


def remove_keys_transitively(d: Dict[K, Any], relation: Dict[K, Collection[K]], seed: K):
    d.pop(seed, None)
    for c in relation[seed]:
        if c in d:
            remove_keys_transitively(d, relation, c)
