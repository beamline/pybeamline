from typing import Any, Optional

from reactivex.disposable import Disposable


class Connectable:
    def __init__(self, connectable_observable: Any):
        self._connectable = connectable_observable
        self._disposable: Optional[Disposable] = None

    def connect(self):
        if self._disposable is None:
            self._disposable = self._connectable.connect()
        return self._disposable

    def disconnect(self):
        if self._disposable is not None:
            try:
                self._disposable.dispose()
            finally:
                self._disposable = None

    def is_connected(self) -> bool:
        return self._disposable is not None