import enum

class Command(enum.Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"

def create_command(command: Command, object_type: str) -> dict:
    """
    Create a command dictionary for the given command and object type.

    Args:
        command (Command): The command to create.
        object_type (str): The type of the object associated with the command.

    Returns:
        dict: A dictionary representing the command.
    """
    return {
        "type": "command",
        "command": command,
        "object_type": object_type
    }
