# Standard library
import sys
from typing import Optional

# Third-party
from colorama import Fore, Style, init
import pandas as pd
from mysql.connector.connection import MySQLConnection

# Local modules
from .config import get_credentials
from .destination import connect_destination
from .source import connect_source
from .sync import sync_data
from .utils import display_pipeline_banner, get_time_stamp
from .validators import validate_arguments

# Initialize colorama with autoreset to automatically clear styles after
# each print
init(autoreset=True)


def main(file: str) -> None:
    """
    Main execution flow for the data pipeline.

    Steps:
        1. Read source CSV file into a pandas DataFrame.
        2. Prompt user for MySQL server credentials.
        3. Establish connection to the MySQL destination.
        4. Sync data from the CSV source to the MySQL database.
        5. Handle success/failure and close the connection.

    Args:
        file (tuple): The name and path of the CSV file to read data from.
    """
    # Step 1: Connect to the source CSV file
    print(
        f"{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
        f"{Fore.YELLOW}{Style.BRIGHT}[📁 Step 2/5]{Style.RESET_ALL} "
        f"{Fore.YELLOW}🔗 Attempting to connect to the CSV source..."
    )
    source: pd.DataFrame = connect_source(file)
    print("\n")

    # Step 2: Collect MySQL server credentials from user input
    print(
        f"{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
        f"{Fore.YELLOW}{Style.BRIGHT}[🔑 Step 3/5]{Style.RESET_ALL} "
        f"{Fore.YELLOW}🔐 Collecting MySQL Server credentials..."
    )
    config: dict[str, str] = get_credentials()
    print("\n")

    # Step 3: Establish connection to the MySQL server
    print(
        f"{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
        f"{Fore.YELLOW}{Style.BRIGHT}[🗄️  Step 4/5]{Style.RESET_ALL} "
        f"{Fore.YELLOW}🔗 Attempting to connect to the MySQL server..."
    )
    destination: Optional[MySQLConnection] = connect_destination(config)

    if destination and destination.is_connected():
        print(
            f"\n{Fore.GREEN}🚀 All tests passed 🚀. We have successfully "
            "established a connection to your database, and data "
            "synchronization can now begin from your source file to the "
            "destination database.\n"
        )
        print(
            f"\n{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
            f"{Fore.YELLOW}{Style.BRIGHT}[🚀 Step 5/5]{Style.RESET_ALL} "
            f"{Fore.YELLOW}Initiating data sync from 📁 CSV source to 🛢️  "
            "MySQL database..."
        )
        # Step 4: Begin data synchronization from CSV to MySQL database
        sync_data(source, destination, file)
        print(
            f"\n{Style.BRIGHT}{Fore.YELLOW}🎉 All operations were executed "
            "without errors, and your data is now securely stored in the "
            f"destination database.\n"
        )
        # Step 5: Close the MySQL connection
        destination.close()
        print(
            f"{Fore.GREEN}🛢️  Database connection has been safely closed. "
            f"\n{Fore.MAGENTA}🏁 Pipeline execution completed successfully. "
            f"Thank you for using the Data Pipeline Tool! 🚀.\n"
        )
    else:
        # Exit if connection to MySQL failed
        sys.exit(
            f"{Fore.RED}❌ Error: Unable to establish a connection to the "
            f"MySQL database 🗄️. Aborting pipeline.\n"
        )


if __name__ == "__main__":
    source_csv_file: str = validate_arguments()
    display_pipeline_banner()
    main(source_csv_file)
