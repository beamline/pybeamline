from collections.abc import MutableMapping
from typing import Dict, Iterator, Optional

from pybeamline.algorithms.conformance.soft.soft_conformance_status import SoftConformanceStatus


class SoftConformanceReport(MutableMapping):

    def __init__(self, *args, **kwargs):
        self._content: Dict[str, SoftConformanceStatus] = dict()
        if args:
            if len(args) > 1:
                raise TypeError(f"expected at most 1 arguments, got {len(args)}")
            self.update(args[0])

        if kwargs:
            self.update(kwargs)

    def __getitem__(self, key: str) -> SoftConformanceStatus:
        return self._content[key]

    def __setitem__(self, key: str, value: SoftConformanceStatus) -> None:
        self._content[key] = value

    def __delitem__(self, key: str) -> None:
        del self._content[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._content)

    def __len__(self) -> int:
        return len(self._content)

    def is_empty(self) -> bool:
        return len(self._content) == 0

    def contains_key(self, key: str) -> bool:
        return key in self._content

    def contains_value(self, value: SoftConformanceStatus) -> bool:
        return value in self._content.values()

    def put_all(self, mapping: Dict[str, SoftConformanceStatus]) -> None:
        self._content.update(mapping)

    def clear(self) -> None:
        self._content.clear()

    def keys(self):
        return self._content.keys()

    def values(self):
        return self._content.values()

    def items(self):
        return self._content.items()

    def get(self, key: str, default: Optional[SoftConformanceStatus] = None) -> Optional[SoftConformanceStatus]:
        return self._content.get(key, default)

    def put(self, key: str, value: SoftConformanceStatus) -> Optional[SoftConformanceStatus]:
        old_value = self._content.get(key)
        self._content[key] = value
        return old_value

    def remove(self, key: str) -> Optional[SoftConformanceStatus]:
        return self._content.pop(key, None)
