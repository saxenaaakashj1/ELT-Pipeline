# Standard library
import pandas as pd
import sys
from typing import Optional

# Third-party libraries
from colorama import Fore, Style

# Local modules
from mysql.connector.connection import MySQLConnection
from utils import display_pipeline_progress, get_time_stamp
from validators import sanitize_column_name, sanitize_table_name


def sync_data(
    source: pd.DataFrame, destination: MySQLConnection, file: str
) -> None:
    """
    Synchronizes a pandas DataFrame(CSV file) with a MySQL table.

    Steps:
        1. Sanitizes and infers the table name from file name.
        2. Infers a primary key if one exists.
        3. Creates the destination table if it doesn't exist.
        4. Inserts or updates data using upsert.
        5. Performs soft delete for missing records.

    Args:
        source (pd.DataFrame): The source data to sync.
        destination (MySQLConnection): Active MySQL connection object.
        file (str): Source CSV file name (used to derive table name).
    """
    table_name: str = sanitize_table_name(file.split(".csv")[0])
    primary_key: Optional[str] = infer_primary_key(source)
    if primary_key:
        primary_key = sanitize_column_name(primary_key)
    create_table_query: str = create_table_from_dataframe(
        source, table_name, primary_key
    )

    print(
        f"\n{Fore.BLUE}[Note]{Fore.MAGENTA} 🛑 By default, the data from "
        f"{Fore.CYAN}{file}{Style.RESET_ALL} {Fore.MAGENTA}will be loaded "
        f"into the table {Fore.CYAN}'{table_name}'{Style.RESET_ALL}"
        f"{Fore.MAGENTA}.Please ensure that no other table in your database "
        "has the same name to avoid conflicts or data overwriting.\n"
    )
    with destination.cursor() as cursor:
        try:
            # Create the table if it does not exist
            display_pipeline_progress(
                f"{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
                f"🔧 Ensuring table {Fore.CYAN}'{table_name}'{Style.RESET_ALL}"
                " exists 🔄",
                3,
            )
            cursor.execute(create_table_query)
            print(
                f"{Fore.GREEN}Table {Fore.CYAN}'{table_name}'{Style.RESET_ALL}"
                f" {Fore.GREEN}is ready for data synchronization. ✅\n"
            )

            # Synchronize data
            print(
                f"{Style.DIM}{get_time_stamp()}{Style.RESET_ALL} "
                "📤 Synchronizing data with the database ..."
            )
            display_pipeline_progress("Sync in progress 🔄", 5)

            # Perform upsert
            upsert_data(cursor, source, table_name)

            # Perform soft delete
            perform_soft_delete(cursor, source, table_name, primary_key)

            destination.commit()
            print(f"{Fore.GREEN}\nSync completed successfully. ✅")
            print(
                f"\n{Fore.BLUE}📊 Load Summary: {Style.RESET_ALL}"
                f"[{Fore.YELLOW}Table {Fore.CYAN}'{table_name}' "
                f"{Style.RESET_ALL}| {Fore.YELLOW}Rows: {Fore.CYAN}"
                f"{len(source)}]"
            )
        except Exception as e:
            destination.rollback()
            sys.exit(
                f"\n\n{Fore.RED}❌ Error during data synchronization - {e}"
                f"\n{Fore.YELLOW}🔴 Aborting pipeline.\n"
            )


def infer_primary_key(source: pd.DataFrame) -> Optional[str]:
    """
    Attempts to automatically infer a primary key column.

    Rules:
        - Must be unique and non-null.
        - Preference given to numeric or datetime columns.

    Args:
        source (pd.DataFrame): DataFrame to analyze.

    Returns:
        Optional[str]: Inferred column name or None.
    """
    # Identify columns with unique and non-null values
    unique_columns: list[str] = [
        col
        for col in source.columns
        if source[col].is_unique and source[col].notna().all()
    ]
    if len(unique_columns) == 1:
        return unique_columns[0]

    # If multiple unique columns, prefer numeric types
    numeric_columns = [
        col
        for col in unique_columns
        if pd.api.types.is_numeric_dtype(source[col])
    ]
    if numeric_columns:
        return numeric_columns[0]

    # If numeric columns are not found, prefer datetime types
    date_columns = [
        col
        for col in unique_columns
        if pd.api.types.is_datetime64_any_dtype(source[col])
    ]
    return (
        date_columns[0]
        if date_columns
        else (unique_columns[0] if unique_columns else None)
    )


def create_table_from_dataframe(
    source: pd.DataFrame, table_name: str, primary_key: Optional[str]
) -> str:
    """
    Generates a CREATE TABLE SQL statement based on a DataFrame schema.

    Args:
        source (pd.DataFrame): Source DataFrame with schema.
        table_name (str): Name of the destination MySQL table.
        primary_key (Optional[str]): Name of the primary key column.

    Returns:
        str: SQL CREATE TABLE statement.
    """
    # When you define a column as BOOLEAN in MySQL, it is automatically
    # created as TINYINT(1).
    dtype_mapping = {
        "int64": "INT",
        "float64": "FLOAT",
        "datetime64[ns]": "DATETIME",
        "bool": "TINYINT(1)",
        "object": "TEXT",
    }

    columns: list[str] = [
        f"{sanitize_column_name(col)} "
        f"{dtype_mapping.get(str(dtype), 'VARCHAR(255)')}"
        for col, dtype in source.dtypes.items()
    ]

    columns.append("synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    columns.append(
        "last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE "
        "CURRENT_TIMESTAMP"
    )
    columns.append("is_deleted BOOLEAN DEFAULT FALSE")

    if primary_key:
        columns.append(f"PRIMARY KEY ({primary_key})")
    else:
        print(
            f"{Fore.YELLOW}⚠️ Warning: No primary key detected. Duplicate rows "
            "may be inserted."
        )

    columns_definition: str = ", ".join(columns)
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition});"


def upsert_data(cursor, source: pd.DataFrame, table_name: str) -> None:
    """
    Performs bulk upsert (insert or update) into the target table.

    Args:
        cursor: MySQL cursor object.
        source (pd.DataFrame): Data to insert.
        table_name (str): Target MySQL table.
    """
    column_names: str = ", ".join(
        [sanitize_column_name(col) for col in source.columns]
    )
    value_placeholders: str = ", ".join(["%s"] * len(source.columns))
    update_assignments: str = ", ".join(
        [
            f"{sanitize_column_name(col)}=VALUES({sanitize_column_name(col)})"
            for col in source.columns
            if col not in ["synced_at", "is_deleted"]
        ]
    )

    upsert_query = (
        f"INSERT INTO {table_name} "
        f"({column_names}, synced_at, last_modified, is_deleted) "
        f"VALUES ({value_placeholders}, NOW(), NOW(), FALSE) "
        f"ON DUPLICATE KEY UPDATE "
        f"{update_assignments}, last_modified=NOW(), is_deleted=FALSE;"
    )

    data_to_insert: list[tuple] = [
        tuple(row) for row in source.itertuples(index=False)
    ]
    BATCH_SIZE = 1000
    for i in range(0, len(data_to_insert), BATCH_SIZE):
        batch = data_to_insert[i : i + BATCH_SIZE]
        cursor.executemany(upsert_query, batch)


def perform_soft_delete(
    cursor, source: pd.DataFrame, table_name: str, primary_key: Optional[str]
) -> None:
    """
    Performs a soft delete by marking rows as deleted that exist in the
    DB but not in the source DataFrame.

    Args:
        cursor: MySQL cursor object.
        source (pd.DataFrame): Current source data.
        table_name (str): Name of target MySQL table.
        primary_key (Optional[str]): Primary key column name.
    """
    if not primary_key:
        print(f"{Fore.YELLOW}⚠️ Skipping soft delete: No primary key detected.")
        return

    temp_table_name = sanitize_table_name(f"{table_name}_temp")
    temp_table_create_query = (
        f"CREATE TEMPORARY TABLE {temp_table_name} ("
        f"{primary_key} VARCHAR(255)"
        ");"
    )
    temp_table_insert_query = (
        f"INSERT INTO {temp_table_name} ({primary_key}) VALUES (%s);"
    )
    soft_delete_query = (
        f"UPDATE {table_name} dest "
        f"SET dest.is_deleted = TRUE, dest.last_modified = NOW() "
        f"WHERE dest.is_deleted = FALSE "
        f"AND NOT EXISTS ("
        f"    SELECT 1 FROM {temp_table_name} temp "
        f"    WHERE temp.{primary_key} = dest.{primary_key}"
        f");"
    )

    source_primary_keys = [
        (row[primary_key],) for row in source.to_dict(orient="records")
    ]

    cursor.execute(temp_table_create_query)
    cursor.executemany(temp_table_insert_query, source_primary_keys)
    cursor.execute(soft_delete_query)
    cursor.execute(f"DROP TEMPORARY TABLE {temp_table_name};")
