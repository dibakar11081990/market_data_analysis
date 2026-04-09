from datetime import datetime
import inspect
import os


def log(message: str, level: str = "INFO"):
    """
    Logs a message with timestamp, level, caller file, and function name.
    
    Parameters:
        message (str): The log message.
        level (str): Log level, e.g., INFO, WARNING, ERROR (default: INFO).
    """
    # Get current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level = level.upper()
    
    # Get the caller's stack frame
    frame = inspect.stack()[1]
    filename = os.path.basename(frame.filename)
    function_name = frame.function

    # Format the log output
    # print(f"[{timestamp}] [{level}] [{filename} -> {function_name}()] >>>>> {message}")
    print(f"[{filename} -> {function_name}()] >>>>> {message}")


def verbose(message: str, title: str = "INFO", width: int = 60, border_char: str = "-"):
    """
    Prints the incoming message in a formatted, verbose style.

    Parameters:
        message (str): The message to print.
        title (str): Optional title to show above the message (default: "INFO").
        width (int): Total width of the output block (default: 60).
        border_char (str): Character to use for the top/bottom border (default: "-").
    """
    border = border_char * width
    centered_title = f" {title.upper()} ".center(width, border_char)
    
    print(border)
    print(centered_title)
    for line in message.split("\n"):
        print(f"{line}")
    print(border)