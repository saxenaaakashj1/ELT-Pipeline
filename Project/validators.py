# Standard library
import ipaddress
import os
import re
import sys
from typing import Callable

# Third-party libraries
from colorama import Fore, Style

# Local modules
from constants import VALID_COLUMN_NAME, VALID_TABLE_NAME


def validate_arguments() -> str:
    """
    Validates command-line arguments and ensures the input is a CSV
    file.

    Returns:
        str: The name of the CSV file extracted from the full path.

    Exits:
        If incorrect arguments are passed or if the file is not a CSV.
    """
    if len(sys.argv) != 2 or not sys.argv[1].endswith(".csv"):
        sys.exit(
            f"{Fore.RED}\n🚨 Usage: python <script.py> <path_to_csv_file>"
            f"{Fore.YELLOW}\n📌 Input file must be in CSV format.\n"
        )

    file_path = sys.argv[1]
    file_name = os.path.basename(file_path)
    return file_name


def validate_credentials(config: dict[str, str]) -> None:
    """
    Validates a dictionary of MySQL credential fields.

    Args:
        config (dict[str, str]): Dictionary with host, port, user,
        password, and database keys.

    Raises:
        ValueError: If any credential fails validation.
    """
    validation_rules: dict[str, Callable[[str], str]] = {
        "host": validate_host,
        "port": validate_port,
        "user": validate_user,
        "password": validate_password,
        "database": validate_database,
    }
    for key, rule in validation_rules.items():
        rule(config[key])


def validate_host(host: str) -> str:
    """
    Validates that a host is a valid IP address, domain name, or
    'localhost'.

    Args:
        host (str): Host address provided by the user.

    Returns:
        str: Validated host.

    Raises:
        ValueError: If the host is invalid.
    """
    if host == "localhost":
        return host
    domain_pattern = re.compile(
        r"^(?!-)(?!.*-$)(?:[a-zA-Z0-9-]{1,63}\.?)+[a-zA-Z]{2,}$"
    )
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        if domain_pattern.match(host):
            return host
    raise ValueError(
        f"{Fore.RED}🚫 Error: The provided host {Fore.CYAN}'{host}' "
        f"{Fore.RED}is invalid.{Style.RESET_ALL}\n"
        f"{Fore.GREEN}🛠️  Solution: A valid hostname must follow:\n"
        f"\t{Fore.GREEN}• A qualified {Fore.CYAN}domain name "
        f"(e.g., 'example.com') {Fore.GREEN}or {Fore.CYAN}'localhost'\n"
        f"\t{Fore.GREEN}• A {Fore.CYAN}IPv4 (e.g., '192.168.1.1') "
        f"{Fore.GREEN}or {Fore.CYAN}IPv6 (e.g., '2001:db8::ff00:42:8329')"
        f"{Style.RESET_ALL}"
    )


def validate_port(port: str) -> str:
    """
    Validates that a port is a number between 1 and 65535.

    Args:
        port (str): Port number as a string.

    Returns:
        str: Validated port.

    Raises:
        ValueError: If the port is out of range or not numeric.
    """
    try:
        if not 1 <= int(port) <= 65535:
            raise ValueError
    except ValueError:
        raise ValueError(
            f"{Fore.RED}🚫 Error: The provided port {Fore.CYAN}'{port}' "
            f"{Fore.RED}is invalid.{Style.RESET_ALL}\n"
            f"{Fore.GREEN}🛠️  Solution: A valid port number must be between "
            f"{Fore.CYAN}1{Fore.GREEN} and {Fore.CYAN}65535{Fore.GREEN}."
            f"{Style.RESET_ALL}"
        )
    return port


def validate_user(user: str) -> str:
    """
    Validates the MySQL username for proper format and restrictions.

    Args:
        user (str): MySQL username.

    Returns:
        str: Validated username.

    Raises:
        ValueError: If the username is invalid or uses reserved words.
    """
    reserved_words: set[str] = {
        "root",
        "admin",
        "mysql",
        "information_schema",
    }
    if (
        not user
        or len(user) > 32
        or user.lower() in reserved_words
        or not re.match(r"^[a-zA-Z][\w.-]*$", user)
    ):
        raise ValueError(
            f"{Fore.RED}🚫 Error: The provided username {Fore.CYAN}'{user}' "
            f"{Fore.RED}is invalid.{Style.RESET_ALL}\n"
            f"{Fore.GREEN}🛠️  Solution: A valid username must:\n"
            f"\t{Fore.GREEN}• Start with a {Fore.CYAN}letter (a-z or A-Z)\n"
            f"\t{Fore.GREEN}• Contain only {Fore.CYAN}letters{Fore.GREEN}, "
            f"{Fore.CYAN}numbers{Fore.GREEN}, {Fore.CYAN}underscores (_)"
            f"{Fore.GREEN}, {Fore.CYAN}dashes (-){Fore.GREEN}, or {Fore.CYAN}"
            "periods (.)\n"
            f"\t{Fore.GREEN}• Not exceed {Fore.CYAN}32 characters\n"
            f"\t{Fore.GREEN}• Not use {Fore.CYAN}reserved words "
            f"(e.g., root, admin) {Style.RESET_ALL}"
        )
    return user


def validate_password(password: str) -> str:
    """
    Validates password strength using minimum length and character rules.

    Args:
        password (str): User's password.

    Returns:
        str: Validated password.

    Raises:
        ValueError: If password is too weak or invalid.
    """
    if len(password) < 8 or not re.search(
        r"^(?=.*[a-zA-Z])(?=.*[0-9])(?=.*[!@#$%^&*(),.?\":{}|<>]).{8,}$",
        password,
    ):
        raise ValueError(
            f"{Fore.RED}🚫 Error: The provided password is invalid."
            f"{Style.RESET_ALL}\n"
            f"{Fore.GREEN}🛠️  Solution: A valid password must be at least "
            f"{Fore.CYAN}8 characters{Fore.GREEN} long and include:\n"
            f"\t{Fore.GREEN}• At least {Fore.CYAN}one letter (a-z or A-Z)\n"
            f"\t{Fore.GREEN}• At least {Fore.CYAN}one number (0-9)\n"
            f"\t{Fore.GREEN}• At least {Fore.CYAN}one special character "
            f"(e.g., !, @, #, $){Style.RESET_ALL}"
        )
    return password


def validate_database(database: str) -> str:
    """
    Validates that a MySQL database name follows naming conventions.

    Args:
        database (str): The database name.

    Returns:
        str: Validated database name.

    Raises:
        ValueError: If the name is invalid or too long.
    """
    if (
        not database
        or len(database) > 64
        or not re.match(r"^[a-zA-Z_]\w*$", database)
    ):
        raise ValueError(
            f"{Fore.RED}🚫 Error: The provided database name {Fore.CYAN}'"
            f"{database}'{Fore.RED} is invalid.{Style.RESET_ALL}\n"
            f"{Fore.GREEN}🛠️  Solution: A valid database name must:\n"
            f"\t{Fore.GREEN}• Start with a {Fore.CYAN}letter{Fore.GREEN} or "
            f"an {Fore.CYAN}underscore (_)\n"
            f"\t{Fore.GREEN}• Contain only {Fore.CYAN}letters{Fore.GREEN}, "
            f"{Fore.CYAN}numbers{Fore.GREEN}, or {Fore.CYAN}underscores (_)\n"
            f"\t{Fore.GREEN}• Not exceed {Fore.CYAN}64 characters"
            f"{Style.RESET_ALL}"
        )
    return database


def sanitize_table_name(table_name: str) -> str:
    """
    Validates that the table name matches allowed patterns.

    Args:
        table_name (str): Name of the table to sanitize.

    Returns:
        str: Validated table name.

    Raises:
        ValueError: If the name is not allowed by the regex.
    """
    if not VALID_TABLE_NAME.match(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name


def sanitize_column_name(column_name: str) -> str:
    """
    Validates that the column name matches allowed patterns.

    Args:
        column_name (str): Name of the column to sanitize.

    Returns:
        str: Validated column name.

    Raises:
        ValueError: If the name is not allowed by the regex.
    """
    if not VALID_COLUMN_NAME.match(column_name):
        raise ValueError(f"Invalid column name: {column_name}")
    return column_name
