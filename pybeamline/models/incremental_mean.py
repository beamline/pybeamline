class IncrementalMean:
    """Apache Commons Math Mean equivalent (empty-safe)."""

    def __init__(self):
        self._count = 0
        self._mean = 0.0

    def increment(self, value: float) -> None:
        self._count += 1
        self._mean += (value - self._mean) / self._count

    def get_result(self) -> float:
        return self._mean if self._count > 0 else 0.0
