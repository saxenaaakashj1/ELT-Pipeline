# Standard library
import datetime
import time

# Third-party libraries
from colorama import Fore, Style
from tqdm import tqdm


def display_pipeline_banner() -> None:
    """Displays a visual pipeline banner and initialization message."""
    time.sleep(1)
    print(
        "\n\n\t\t\t"
        f"{Fore.CYAN}{Style.BRIGHT} 🚀 Welcome to Data Pipeline Tool! 🚀"
        "\n"
    )
    print(
        r"""
             (Source)                                       (Destination)    
           +---------+           +-----------+          +--------------------+
           |  Read   |   ---->   |  Extract  |   ---->  |  Load to MySQL DB  |
           |  CSV    |           |   Data    |          |    (Final Load)    |
           +---------+           +-----------+          +--------------------+
                |  📁                  |  🔄                     🗄️ |
                +----------------------+---------------------------+
                                 Data Pipeline
    """
    )
    print("\n")

    # Initializing Pipeline
    time.sleep(1)

    print(
        f"{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
        f"{Fore.YELLOW}{Style.BRIGHT}[📁 Step 1/5]{Style.RESET_ALL} "
        f"{Fore.YELLOW}🛠️  Initializing data pipeline..."
    )
    display_pipeline_progress("Preparing pipeline environment 🔄", 3)
    print("\n")


def get_time_stamp() -> str:
    """
    Returns the current timestamp in a formatted string.

    Returns:
        str: Timestamp formatted as [YYYY-MM-DD HH:MM:SS].
    """
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def display_pipeline_progress(task: str, duration: int) -> None:
    """
    Displays a progress bar for a current task.

    Args:
        task (str): Description of the current task.
        duration (int): Duration in seconds for the simulated progress.
    """
    for _ in tqdm(
        range(duration),
        desc=task,
        total=duration,
        bar_format="{l_bar}{bar} | {elapsed}s",
    ):
        time.sleep(1)
