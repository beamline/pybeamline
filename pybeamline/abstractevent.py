from abc import ABC, abstractmethod

class AbstractEvent(ABC):
    @abstractmethod
    def to_dict(self):
        """
        Convert the event to a dictionary representation.
        """
        pass