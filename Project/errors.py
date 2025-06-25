# Standard library
import sys

# Third-party libraries
from colorama import Fore, Style
import mysql
from mysql.connector import errorcode
import pandas as pd


def handle_errors(e: Exception, error_type: str, file: str) -> None:
    """
    Handles file-related and pandas-related errors with user-friendly,
    color-coded messages and exits the pipeline gracefully.

    Args:
        e (Exception): The raised exception instance.
        error_type (str): Type of error context ('file' or 'pandas').
        file (str): The name of the CSV file being processed.
    """
    errors: dict[str, dict[type[Exception], str]] = {
        "file": {
            FileNotFoundError: (
                f"\n{Fore.RED}🚫 Error: The specified source file "
                f"{Fore.CYAN}'{file}'{Fore.RED} was not found. "
                f"{Style.RESET_ALL}\n"
                f"{Fore.GREEN}👉 Action: Verify the file path and ensure the "
                f"file {Fore.CYAN}'{file}'{Fore.GREEN} exists."
            ),
            PermissionError: (
                f"\n{Fore.RED}🚫 Error: Insufficient permissions to access "
                f"the file {Fore.CYAN}'{file}'{Fore.RED}.{Style.RESET_ALL}\n"
                f"{Fore.GREEN}👉 Action: Check file permissions or try "
                f"running the pipeline with appropriate access."
            ),
        },
        "pandas": {
            pd.errors.EmptyDataError: (
                f"\n{Fore.RED}🚫 Error: The file {Fore.CYAN}'{file}'{Fore.RED}"
                f" is empty. The pipeline cannot sync {Fore.CYAN}empty files"
                f"{Fore.RED}.{Style.RESET_ALL}\n"
                f"{Fore.GREEN}👉 Action: Provide a valid CSV file containing "
                f"data for pipeline synchronization.{Style.RESET_ALL}"
            ),
            pd.errors.ParserError: (
                f"\n{Fore.RED}🚫 Error: The file {Fore.CYAN}'{file}'{Fore.RED}"
                f" could not be parsed due to formatting issues.\n "
                f"{Style.RESET_ALL}"
                f"{Fore.GREEN}👉 Action: Check if the file is a valid CSV and "
                f"not corrupted.{Style.RESET_ALL}"
            ),
        },
    }
    if error_type not in errors:
        sys.exit(
            f"\n{Fore.RED}🚫 Error: Unmapped error type "
            f"{Fore.CYAN}'{error_type}'{Fore.RED}.{Style.RESET_ALL}"
            f"\n{Fore.GREEN}👉 Action: Please update the {Fore.CYAN}'errors' "
            f"{Fore.GREEN}dictionary.{Style.RESET_ALL}"
            f"\n{Fore.YELLOW}🔴 Aborting pipeline.\n"
        )
    # Fetch the appropriate error message
    message = errors[error_type].get(
        type(e),
        f"\n{Fore.RED}❌ Critical Pipeline Error - "
        "Unexpected issue encountered: "
        f"{e}.{Style.RESET_ALL}",
    )
    sys.exit(f"\n{message}\n{Fore.YELLOW}🔴 Aborting pipeline.\n")


def handle_mysql_errors(
    e: mysql.connector.Error, is_db_error: bool = False
) -> None:
    """
    Handles MySQL-related connection and database errors with actionable
    messages.

    Args:
        e (mysql.connector.Error): The MySQL exception instance.
        is_db_error (bool): Whether this is a database related error.
    """
    errors = {
        errorcode.ER_ACCESS_DENIED_ERROR: f"{Fore.RED}🚫 Access Denied: "
        f"Incorrect username or password.{Style.RESET_ALL}"
        f"\n{Fore.GREEN}🛠️  Solution: Double-check your credentials and try "
        f"again.{Style.RESET_ALL}",
        errorcode.CR_CONN_HOST_ERROR: f"{Fore.RED}🚫 Connection Error: "
        f"Unable to connect to the MySQL server.{Style.RESET_ALL}"
        f"\n{Fore.GREEN}🛠️  Solution: Check if the MySQL server is running and "
        f"accessible at the specified host.{Style.RESET_ALL}",
        111: f"{Fore.RED}🚫 Connection Refused: MySQL might not be running "
        f"or the port is blocked.{Style.RESET_ALL}"
        f"\n{Fore.GREEN}🛠️  Solution: Ensure MySQL is running and accessible "
        f"on the correct port.{Style.RESET_ALL}",
        2003: f"{Fore.RED}🚫 Connection Timed Out: The server may be "
        f"unreachable or the port closed.{Style.RESET_ALL}"
        f"\n{Fore.GREEN}🛠️  Solution: Verify network connection and that MySQL "
        f"is accessible on the specified host and port.{Style.RESET_ALL}",
    }
    if is_db_error and e.errno == errorcode.ER_BAD_DB_ERROR:
        sys.exit(
            f"\n\n{Fore.RED}🚫 Database Not Found: The specified database does"
            f" not exist on the server.{Style.RESET_ALL}"
            f"\n{Fore.GREEN}🛠️  Solution: Verify the database name or create "
            f"it if it doesn't exist.{Style.RESET_ALL}"
            f"\n{Fore.YELLOW}🔴 Aborting pipeline.\n"
        )

    if e.errno not in errors:
        sys.exit(
            f"\n\n{Fore.RED}🚫 Unexpected Error - {e}\n{Style.RESET_ALL}"
            f"{Fore.GREEN}🛠️  Solution: Consult the {Fore.CYAN}📘 MySQL "
            f"documentation - https://dev.mysql.com/doc/ {Fore.GREEN}or "
            f"{Fore.CYAN}support{Fore.GREEN} for further troubleshooting.\n"
            f"{Style.RESET_ALL}"
            f"{Fore.YELLOW}🔴 Aborting pipeline.\n"
        )
    sys.exit(
        f"\n\n{Fore.RED}{errors[e.errno]} "
        f"\n{Fore.YELLOW}🔴 Aborting pipeline.\n"
    )
