from typing import Mapping, TypeVar, Dict, Iterable, Collection, Any, MutableMapping, MutableSet

K = TypeVar('K')
V = TypeVar('V')


def filter_dict(origin: Mapping[K, V], keys: Iterable[K]) -> Dict[K, V]:
    return {k: origin[k] for k in keys}


Missing = object()


def remove_keys_transitively(d: MutableMapping[K, Any], relation: Mapping[K, Collection[K]], seed: K,
                             removed_keys_sink: MutableSet[K]):
    if d.pop(seed, Missing) is not Missing:
        removed_keys_sink.add(seed)
    for c in relation[seed]:
        if c in d:
            remove_keys_transitively(d, relation, c, removed_keys_sink)
