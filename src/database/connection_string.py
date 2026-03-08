import struct
from typing import Any, Dict


def parse_sqlserver_connection_string(connection_string):
    """
    Parses a SQL Server connection string into a dictionary.

    Args:
    connection_string (str): The SQL Server connection string.

    Returns:
    dict: A dictionary with the parsed connection string elements.
    """
    connection_string = connection_string.strip()
    pairs = connection_string.split(";")
    parsed_results = {}

    for pair in pairs:
        if "=" in pair:
            key, value = pair.split("=", 1)
            parsed_results[key.strip()] = value.strip()

    if "AccessToken" not in parsed_results:
        parsed_results["AccessToken"] = None

    return parsed_results


def build_odbc_connection_string(parsed_dict):
    """
    Builds a pyodbc connection string from a parsed dictionary.

    Args:
    parsed_dict (dict): A dictionary with parsed connection string elements.

    Returns:
    str: A pyodbc connection string.
    """
    odbc_str = ";".join([f"{key}={value}" for key, value in parsed_dict.items()])
    return odbc_str


def connection_string_token(token):
    if not token:
        return ""

    tokenb = bytes(token, "UTF-8")
    exptoken = b""

    for i in tokenb:
        exptoken += bytes({i})
        exptoken += bytes(1)

    tokenstruct = struct.pack("=i", len(exptoken)) + exptoken

    return tokenstruct


def valid_connection_string(connection_string: str) -> Dict[str, Any]:
    """
    Validates and prepares the SQL Server connection string.

    :param connection_string: A semi-colon delimited SQL Server connection string.
    :return: A dict with keys:
        - "connection_string": the ODBC-formatted connection string
        - "connect_args": optional arguments for pyodbc.connect()
    :raises ValueError: If the connection string is invalid or incomplete
    """
    try:
        connection_string_dict = parse_sqlserver_connection_string(connection_string)
    except Exception as e:
        raise ValueError(f"Failed to parse connection string: {e}")

    try:
        odbc_connection_string = build_odbc_connection_string(connection_string_dict)
    except Exception as e:
        raise ValueError(f"Failed to build ODBC connection string: {e}")

    connect_args = {}
    access_token = connection_string_dict.get("AccessToken")

    if access_token:
        try:
            token_bytes = connection_string_token(token=access_token)
            connect_args = {
                "attrs_before": {1256: token_bytes}  # SQL_COPT_SS_ACCESS_TOKEN
            }
        except Exception as e:
            raise ValueError(f"Invalid access token in connection string: {e}")

    return {"connection_string": odbc_connection_string, "connect_args": connect_args}
