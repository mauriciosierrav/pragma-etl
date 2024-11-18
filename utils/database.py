"""Module to interact with a MySQL database"""

from typing import Any
import pymysql
from pymysql.err import MySQLError
from config import logger


class MySQLDatabase:
    """Class to interact with a MySQL database"""

    def __init__(self, host: str, user: str, password: str, db: str):
        """Initialize the MySQLDatabase class with the host, user, password and db"""
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.connection = None

    def __enter__(self):
        """Establish a connection to the database"""
        try:
            self.connection = pymysql.connect(
                host=self.host, user=self.user, password=self.password, db=self.db
            )
            logger.info(f"Connected successfully to the RDS '{self.host}'")
            return self.connection
        except Exception as e:
            logger.fatal(f"Error connecting to the RDS '{self.host}': {e}")
            raise

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Close the connection to the database"""
        if self.connection:
            self.connection.close()
            logger.info(f"Connection to the database '{self.host}' has been closed")

    def __execute_sql__(
        self,
        sql: str,
        sql_args: list[tuple[Any, ...]] | tuple[Any, ...] | None = None,
        fetch: int = 0,
        commit: bool = True,
    ):
        """Execute a SQL statement

        Parameters
        ----------
        sql : str
            The SQL statement to execute
        sql_args : list[tuple[Any, ...]] | tuple[Any, ...] | None
            The arguments to pass to the SQL statement
        fetch : int
            The number of rows to fetch. If -1, fetch all rows. If 0 or less, do not fetch any rows.
        commit : bool
            Whether to commit the transaction or not

        Returns
        -------
        result : tuple[tuple[Any, ...], ...] | None
            The result of the SQL statement (only for SELECT statements)
        """
        try:
            # Ensure the connection is established before executing the query
            if self.connection is None:
                raise MySQLError("Connection to the database has not been established")

            sql_statement = " ".join(line.strip() for line in sql.splitlines() if line.strip())
            if sql_args:
                sql_statement = sql_statement % sql_args
            logger.debug(f"Executing SQL statement '{sql_statement[0:200]}'...")
            cnx = self.connection
            with cnx.cursor() as cursor:

                # Execute the SQL statement
                if isinstance(sql_args, list):
                    cursor.executemany(sql, sql_args)
                elif isinstance(sql_args, tuple):
                    cursor.execute(sql, sql_args)
                else:
                    cursor.execute(sql)

                # Fetch the results based on the fetch parameter (only for SELECT statements)
                if fetch == -1:
                    result = cursor.fetchall()
                elif fetch >= 1:
                    result = cursor.fetchmany(fetch)
                else:
                    result = None

                # Commit the transaction if the commit parameter is set to True
                if commit:
                    cnx.commit()
            logger.debug("SQL statement executed successfully")
            return result

        except Exception as e:
            logger.fatal(f"Error executing SQL statement: {e}")
            raise

    def select(
        self,
        sql: str,
        sql_args: tuple[Any, ...] | None = None,
        num_rows: int = 1,
    ):
        """
        Execute a SELECT statement

        Parameters
        ----------
        sql : str
            The SELECT statement to execute
        sql_args : tuple[Any, ...] | None
            The arguments to pass to the SQL statement
        num_rows : int
            The number of rows to fetch. If -1, fetch all rows. If 0 or less, do not fetch any rows.
            Default is 1

        Examples
        --------
        >>> db = MySQLDatabase("host", "user", "password", "database")
            with db:
                db.select("SELECT * FROM TABLE_NAME")
                db.select("SELECT * FROM TABLE_NAME", num_rows=10)
                db.select("SELECT * FROM TABLE_NAME WHERE COLUMN_NAME1 = %s AND COLUMN_NAME_2 = %s", ("value", "value_2"), num_rows=10)
        """
        return self.__execute_sql__(sql, sql_args, fetch=num_rows, commit=False)

    def insert(
        self, sql: str, sql_args: list[tuple[Any, ...]] | tuple[Any, ...] | None = None
    ):
        """
        Execute an INSERT statement. Insert rows into the database

        Parameters
        ----------
        sql : str
            The INSERT statement to execute
        sql_args : list[tuple[Any, ...]] | tuple[Any, ...] | None
            The arguments to pass to the SQL statement

        Examples
        --------
        >>> db = MySQLDatabase("host", "user", "password", "database")
            with db:
                db.insert("INSERT INTO TABLE_NAME (COLUMN_NAME1, COLUMN_NAME2) VALUES ('value1', 'value2')")
                db.insert("INSERT INTO TABLE_NAME (COLUMN_NAME1, COLUMN_NAME2) VALUES (%s, %s)", ("value1", "value2"))
                db.insert("INSERT INTO TABLE_NAME (COLUMN_NAME1, COLUMN_NAME2) VALUES (%s, %s)", [("value1", "value2"), ("value3", "value4")])
        """
        return self.__execute_sql__(sql, sql_args)

    def update(
        self, sql: str, sql_args: list[tuple[Any, ...]] | tuple[Any, ...] | None = None
    ):
        """
        Execute an UPDATE statement. Update rows in the database

        Parameters
        ----------
        sql : str
            The UPDATE statement to execute
        sql_args : list[tuple[Any, ...]] | tuple[Any, ...] | None
            The arguments to pass to the SQL statement

        Examples
        --------
        >>> db = MySQLDatabase("host", "user", "password", "database")
            with db:
                db.update("UPDATE TestTable SET COLUMN_NAME1 = 1 WHERE COLUMN_NAME2 = 'value1'")
                db.update("UPDATE TestTable SET COLUMN_NAME1 = %s WHERE COLUMN_NAME2 = %s", (1, "value1"))
                db.update("UPDATE TestTable SET COLUMN_NAME1 = %s WHERE COLUMN_NAME2 = %s", [(1, "value1"), (2, "value2")])
        """
        return self.__execute_sql__(sql, sql_args)

    def delete(
        self, sql: str, sql_args: list[tuple[Any, ...]] | tuple[Any, ...] | None = None
    ):
        """
        Execute a DELETE statement. Delete rows from the database

        Parameters
        ----------
        sql : str
            The DELETE statement to execute
        sql_args : list[tuple[Any, ...]] | tuple[Any, ...] | None
            The arguments to pass to the SQL statement

        Examples
        --------
        >>> db = MySQLDatabase("host", "user", "password", "database")
            with db:
                db.delete("DELETE FROM TABLE_NAME WHERE COLUMN_NAME1 = 'value1'")
                db.delete("DELETE FROM TABLE_NAME WHERE COLUMN_NAME1 = %s", ("value1",))
                db.delete("DELETE FROM TABLE_NAME WHERE COLUMN_NAME1 = %s", [("value1",), ("value2",)])
                db.delete("DELETE FROM TABLE_NAME WHERE COLUMN_NAME1 = %s and COLUMN_NAME2 = %s", [("value1", "value2"), ("value3", "value4")])
        """
        return self.__execute_sql__(sql, sql_args)

    def create_table(self, sql: str):
        """
        Execute a CREATE TABLE statement. Create a table in the database

        Parameters
        ----------
        sql : str
            The CREATE TABLE statement to execute
        sql_args : tuple[Any, ...] | None
            The arguments to pass to the SQL statement

        Notes
        -----
        The CREATE TABLE statement does not allow placeholders for table names or column names.

        Examples
        --------
        >>> db = MySQLDatabase("host", "user", "password", "database")
            with db:
                db.create_table("CREATE TABLE IF NOT EXISTS TABLE_NAME (COLUMN_NAME1 INT, COLUMN_NAME2 VARCHAR(255))")
                db.create_table(f"CREATE TABLE IF NOT EXISTS {table_name} (COLUMN_NAME1 INT, COLUMN_NAME2 VARCHAR(255))")
        """
        return self.__execute_sql__(sql)

    def drop_table(self, sql: str):
        """
        Execute a DROP TABLE statement. Drop a table from the database

        Parameters
        ----------
        sql : str
            The DROP TABLE statement to execute

        Notes
        -----
            The DROP TABLE statement does not allow placeholders for table names.

        Examples
        --------
        >>> db = MySQLDatabase("host", "user", "password", "database")
            with db:
                db.drop_table("DROP TABLE IF EXISTS TABLE_NAME")
                db.drop_table(f"DROP TABLE IF EXISTS {table_name}")
        """
        return self.__execute_sql__(sql)

    def truncate_table(self, sql: str):
        """
        Execute a TRUNCATE TABLE statement. Truncate a table in the database

        Parameters
        ----------
        sql : str
            The TRUNCATE TABLE statement to execute

        Notes
        -----
        The TRUNCATE TABLE statement does not allow placeholders for table names.

        Examples
        --------
        >>> db = MySQLDatabase("host", "user", "password", "database")
            with db:
                db.truncate_table("TRUNCATE TABLE TABLE_NAME")
                db.truncate_table(f"TRUNCATE TABLE {table_name}")
        """
        return self.__execute_sql__(sql)
