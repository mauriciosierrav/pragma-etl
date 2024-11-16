"""Module providing all the configuration variables and constants for the project"""

import os
from dotenv import load_dotenv


# Get the environment variables defined in the .env file or in the environment
def _get_env_var(var_name: str):
    # Try to get the environment variable
    var = os.getenv(var_name)

    # If it is not defined, try to load the .env file
    if var is None:
        load_dotenv(override=True)  # Cargar el archivo .env si no se ha hecho antes
        var = os.getenv(var_name)

    # If the variable is still not defined, raise an exception
    if var is None:
        raise ValueError(f"The required environment variable {var_name} is not defined")

    return var


DB_HOST = _get_env_var("DB_HOST")
DB_USER = _get_env_var("DB_USER")
DB_PASSWORD = _get_env_var("DB_PASSWORD")
DB_NAME = _get_env_var("DB_NAME")
