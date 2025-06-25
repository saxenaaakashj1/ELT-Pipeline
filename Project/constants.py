# Standard library
import re

# Regex patterns for validating table and column names
VALID_TABLE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
VALID_COLUMN_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
