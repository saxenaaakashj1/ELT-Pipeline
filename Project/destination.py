# Standard library
from typing import Optional, cast

# Third-party libraries
from colorama import Fore, Style
import mysql
from mysql.connector.connection import MySQLConnection
from errors import handle_mysql_errors
from utils import display_pipeline_progress, get_time_stamp


def connect_destination(config: dict[str, str]) -> Optional[MySQLConnection]:
    """
    Establishes a connection to a MySQL server and then connects to a
    specific database.

    Steps:
        1. Connects to the MySQL server using provided credentials.
        2. Attempts to connect to the target database.
        3. Handles connection errors gracefully using custom error
        logic.

    Args:
        config (dict[str, str]): A dictionary of MySQL connection
        parameters, including host, port, user, password, and
        database.

    Returns:
        Optional[MySQLConnection]: A connected MySQLConnection object if
        successful, otherwise None.
    """
    cnx: Optional[MySQLConnection] = None
    try:
        # Extract database from config; remove to avoid issues during
        # initial connection
        database = config.pop("database", None)
        display_pipeline_progress("Establishing connection 🔄", 3)
        cnx = cast(MySQLConnection, mysql.connector.connect(**config))
        if cnx and cnx.is_connected():
            print(
                f"\n{Fore.GREEN}🗄️  Successfully connected to MySQL server at "
                f"{Style.BRIGHT}{config['host']}:{config['port']}. ✅"
            )
        print(
            f"\n{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
            f"{Fore.YELLOW}🛢️  Preparing database environment..."
        )
        if database:
            display_pipeline_progress(
                f"Ensuring database {Fore.CYAN}'{database}'{Style.RESET_ALL} "
                "exist and is ready to use 🔄",
                3,
            )
            try:
                cnx.database = database
                print(
                    f"\n{Fore.GREEN}🛢️  Successfully connected to the "
                    f"{Fore.CYAN}'{database}'{Fore.GREEN} "
                    f"database. ✅"
                )
            except mysql.connector.Error as db_error:
                handle_mysql_errors(db_error, is_db_error=True)
    except mysql.connector.Error as mysql_error:
        handle_mysql_errors(mysql_error)
    return cnx
