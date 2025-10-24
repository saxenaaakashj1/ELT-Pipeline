# Standard library
import getpass
import sys

# Third-party libraries
from colorama import Fore, Style

# Local modules
from .utils import display_pipeline_progress, get_time_stamp
from .validators import validate_credentials


def get_credentials() -> dict[str, str]:
    """
    Prompts the user for MySQL credentials securely and validates them.

    The steps are:
        1. Prompt the user for host, port, username, password, and
        database.
        2. Validate the credentials using custom rules.
        3. Exit the pipeline if validation fails.

    Returns:
        dict[str, str]: A dictionary containing sanitized MySQL config
        values.
    """
    display_pipeline_progress("Preparing secure environment 🔄", 3)
    print(f"\n{Fore.YELLOW}🔑 Please provide MySQL database credentials:\n")
    # Prompt user for database credentials
    config = {
        "host": input("🌐 Host: ").strip(),
        "port": input("🔌 Port (default '3306'): ").strip() or "3306",
        "user": input("👤 User: ").strip(),
        "password": getpass.getpass("🔒 Password (hidden): ").strip(),
        "database": input("🛢️Database: ").strip(),
    }

    print(
        f"\n{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
        f"{Fore.YELLOW}Running basic credential validation 🛡️...\n"
    )
    try:
        display_pipeline_progress("🔎 Validating credentials 🔄", 3)
        validate_credentials(config)
        print(f"\n{Fore.GREEN}Basic credential validation successful. ✅ ")
    except ValueError as validation_error:
        sys.exit(
            f"\n\n{validation_error}\n{Fore.YELLOW}🔴 Aborting pipeline.\n"
            f"{Style.RESET_ALL}"
        )
    except Exception as e:
        sys.exit(
            f"\n{Fore.RED}❌ Unexpected Error: {e}\n{Fore.YELLOW}🔴 Aborting "
            f"pipeline.\n"
        )
    return config
