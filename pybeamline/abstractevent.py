from abc import ABC, abstractmethod

# Facilitates the work of an interface for event handling in a system.
class AbstractEvent(ABC):

    @abstractmethod
    def get_event_name(self):
        """
        Get the name of the event.
        """
        pass

    @abstractmethod
    def get_event_time(self):
        """
        Get the timestamp of the event.
        """
        pass

    @abstractmethod
    def to_dict(self):
        """
        Convert the event to a dictionary representation.
        """
        pass