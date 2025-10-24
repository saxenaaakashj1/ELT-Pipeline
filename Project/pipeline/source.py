# Standard library
import sys
from typing import Optional

# Third-party libraries
from colorama import Fore, Style
import pandas as pd


# Local modules
from .errors import handle_errors
from .schema import display_erd
from .utils import display_pipeline_progress, get_time_stamp


def connect_source(file: str) -> pd.DataFrame:
    """
    Connects to the provided CSV file, validates it, and loads it into a
    DataFrame.

    This function performs the following:
        1. Displays progress while attempting to connect to the file.
        2. Reads the CSV using pandas and validates its integrity.
        3. Handles common file-related and pandas-related errors
        gracefully.
        4. Call display_erd function to display an ERD Diagram
        5. Prints an ingestion summary showing file name, row count, and
        column count.

    Args:
        file (str): The name or path of the CSV file.

    Returns:
        pd.DataFrame: The loaded and validated data from the CSV file.
    """
    file_name = file.rsplit("/", 1)[-1]
    # Step 1: Display progress for initial file connection
    display_pipeline_progress("Establishing connection 🔄", 3)

    # Step 2: Display progress for file discovery and validation
    print(
        f"\n{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
        f"{Fore.YELLOW}🔎 Validating the CSV file {Fore.CYAN}'{file_name}'"
        f"{Fore.YELLOW}..."
    )
    display_pipeline_progress(
        f"Locating, reading, and validating file {Fore.CYAN}'{file_name}'"
        f"{Style.RESET_ALL} 🔄",
        3,
    )

    dataframe: Optional[pd.DataFrame] = None

    try:
        # Attempt to read the CSV file into a DataFrame
        dataframe: pd.DataFrame = pd.read_csv(file)
        print(
            f"\n{Fore.GREEN}File {Fore.CYAN}'{file_name}' {Fore.GREEN}successfully "
            f"validated and loaded into the memory. ✅ \n"
        )
    except (
        FileNotFoundError,
        PermissionError,
    ) as e:
        # Handle file-level issues such as missing file or permission
        # error
        handle_errors(e, "file", file)
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        # Handle CSV-specific data issues such as empty or corrupt
        # content
        handle_errors(e, "pandas", file)
    except Exception as e:
        # Handle any unexpected issue and exit the pipeline
        sys.exit(
            f"\n{Fore.RED}❌ Critical Pipeline Error - "
            f"Unexpected issue encountered: {e}.{Style.RESET_ALL}\n"
            f"{Fore.YELLOW}🔴 Aborting pipeline.\n"
        )

    assert dataframe is not None

    # Step 3: Display ERD-style schema from the loaded DataFrame
    display_erd(dataframe, file)

    # Step 4: Display summary of the ingested file (file, rows & columns)
    print(
        f"{Fore.BLUE}📊 Ingest Summary: "
        f"{Fore.WHITE}[{Fore.YELLOW}File {Fore.CYAN}'{file_name}' "
        f"{Fore.WHITE}| {Fore.YELLOW}Rows: {Fore.CYAN}{len(dataframe)} "
        f"{Fore.WHITE}| {Fore.YELLOW}Columns: {Fore.CYAN}"
        f"{len(dataframe.columns)}{Fore.WHITE}]"
    )

    return dataframe
