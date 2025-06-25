# Third-party libraries
from colorama import Fore, Style
import pandas as pd


# Local modules
from utils import display_pipeline_progress, get_time_stamp


def display_erd(dataframe: pd.DataFrame, file: str) -> None:
    """
    Displays an ERD-style schema summary of the given DataFrame in a
    tabular format.

    This includes:
        - Column (field) names
        - Corresponding data types for each column

    Args:
        dataframe (pd.DataFrame): The DataFrame to inspect.
        file (str): The source filename (used for display purposes).
    """
    print(
        f"{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
        f"{Fore.YELLOW}🔍 Analyzing and fetching the file schema..."
    )
    display_pipeline_progress(
        f"Inspecting {Fore.CYAN}'{file}'{Style.RESET_ALL} schema 🔄", 3
    )

    headers = ["Fields", "Data Type"]
    schema_info = [
        (
            str(col),
            str(dataframe[col].dtype),
        )
        for col in dataframe.columns
    ]

    col_widths = {
        h: max(len(h), max(len(row[i]) for row in schema_info)) + 2
        for i, h in enumerate(headers)
    }

    print(
        f"\n{Fore.YELLOW}📂 File {Fore.CYAN}'{file}' "
        f"{Fore.YELLOW}Schema [ERD]:\n"
    )
    print(
        f"{Style.BRIGHT}{Fore.RED}"
        + "".join(h.ljust(col_widths[h]) for h in headers)
    )
    print(f"{Style.BRIGHT}{Fore.YELLOW}{'-' * sum(col_widths.values())}")

    for name, dtype in schema_info:
        print(
            f"{Fore.RED}{name.ljust(col_widths[headers[0]])}"
            f"{dtype.ljust(col_widths[headers[1]])}"
        )

    print(f"\n{Fore.GREEN}Schema fetched successfully. ✅\n")
