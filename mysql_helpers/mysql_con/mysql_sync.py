""" Handles queries to MySQL using the mysql-python native connector"""
from os import environ
from typing import Union, Optional, List, Tuple
from pathlib import Path
import pandas as pd
import logging
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.errors import InterfaceError

logger = logging.getLogger(f"mysql_helpers:{Path(__file__).name}")


class MySQLConnectorNative:
    """MySQL class helpers with Rlock use"""

    def __init__(
        self,
        db_host: Optional[str] = None,
        db_port: Union[int, str] = None,
        db_user: Optional[str] = None,
        db_password: Optional[str] = None,
        db_name: Optional[str] = None,
        raise_on_warnings: bool = False,
    ):
        self.db_host: str = environ["MYSQL_DB_HOST"] if db_host is None else db_host
        self.db_port: Union[int, str] = (
            environ["MYSQL_DB_PORT"] if db_port is None else str(db_port)
        )
        self.db_user: str = environ["MYSQL_DB_USER"] if db_user is None else db_user
        self.db_password: str = (
            environ["MYSQL_DB_PASS"] if db_password is None else db_password
        )
        self.db_name: str = environ["MYSQL_DB_NAME"] if db_name is None else db_name
        self.raise_on_warnings: bool = raise_on_warnings
        self.mysql_connection: Union[None, MySQLConnection] = None

    def open_connection(self) -> Union[None, MySQLConnection]:
        """Return mysql connection or None if failure to establish one"""
        if self.mysql_connection is not None and self.mysql_connection.is_connected():
            return self.mysql_connection

        try:
            self.mysql_connection = MySQLConnection(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                passwd=self.db_password,
                database=self.db_name,
                get_warnings=True,
                raise_on_warnings=self.raise_on_warnings,
            )
            return self.mysql_connection
        except InterfaceError as ex:
            logger.warning(
                f"Unhandled connection error: can't find MySql server on Host: {self.db_host}, Port: {self.db_port}. Error is: {ex}. Error type is: {type(ex)}."
            )
        except Exception as ex:
            logger.error(f"Unhandled connection error: {ex}")
        return None

    def close_connection(self):
        if self.mysql_connection is not None and self.mysql_connection.is_connected():
            self.mysql_connection.close()

    def fetch_all_as_df(
        self,
        sql_query: str,
        sql_variables: Optional[Tuple] = None,
        close_connection: Optional[bool] = True,
    ) -> Union[pd.DataFrame, None]:
        """
        :param sql_query: the MySQL query
        :param sql_variables: parameters ordered with %s usage in the sql_query
        :param close_connection: close connection after the method ends
        :return: return a pandas DataFrame if there are results or None if error
        """
        if not self.open_connection():
            return None
        try:
            mysql_cursor = self.mysql_connection.cursor()

            mysql_cursor.execute(sql_query, sql_variables)
            result_df = pd.DataFrame(mysql_cursor.fetchall())
            result_df.columns = mysql_cursor.column_names

            mysql_cursor.close()
            return result_df
        except Exception as ex:
            logger.error(
                f"Error while fetching data : {ex}. Exception is {ex.__class__.__name__}"
            )
            return None
        finally:
            if close_connection:
                self.close_connection()

    def fetch_all_as_dicts(
        self,
        sql_query: str,
        sql_variables: Optional[Tuple] = None,
        close_connection: Optional[bool] = True,
    ) -> Union[List[Tuple], None]:
        """
        :param sql_query: the MySQL query
        :param sql_variables: parameters ordered with %s usage in the sql_query
        :param close_connection: close connection after the method ends
        :return: return a pandas DataFrame if there are results or None if error
        """
        if not self.open_connection():
            return None

        try:
            mysql_cursor: MySQLCursor = self.mysql_connection.cursor()
            mysql_cursor.execute(sql_query, sql_variables)
            results = mysql_cursor.fetchall()
            mysql_cursor.close()
            if close_connection:
                self.close_connection()
            return results
        except Exception as ex:
            logger.error(
                f"Error while inserting new score: {ex}. SQL Statement used: {mysql_cursor.statement}"
            )
            return None

    def execute_one_query(
        self,
        sql_query: str,
        sql_variables: Optional[Tuple] = None,
        close_connection: Optional[bool] = True,
    ) -> int:
        """method that handles execute queries: delete update insert
        :param sql_query: the MySQL query
        :param sql_variables: parameters ordered with %s usage in the sql_query
        :param close_connection: close connection after the method ends
        :return: returns the number of rows affected, -1 if connection error ONLY
        """

        if not self.open_connection():
            return -1

        rows_affected: int = 0
        try:
            mysql_cursor: MySQLCursor = self.mysql_connection.cursor()
            mysql_cursor.execute(sql_query, sql_variables)
            rows_affected = mysql_cursor.rowcount
            self.mysql_connection.commit()
            mysql_cursor.close()
            if close_connection:
                self.close_connection()
        except Exception as ex:
            logger.error("Error ({ex.__class__.__name__}) while executing query: {ex}")
            logger.error(f"Variables used: {sql_variables}")
            try:
                print(
                    f'Statement used:\n"{mysql_cursor.statement}" ',
                )
            # TODO: use the appropriate Excetion code for undefined variables
            except Exception as ex:
                logger.error(
                    f"Error ({ex.__class__.__name__}) while executing sql statement: {ex}"
                )

        return rows_affected

    #:TODO: add execute many using prepared statement


if __name__ == "__main__":
    from dotenv import load_dotenv
    from mysql_helpers.app_config import logging_config
    logging_config()

    load_dotenv()
    my_getter = MySQLConnectorNative()
    print(
        my_getter.fetch_all_as_df(
            sql_query="SELECT * FROM tbl_proxy_url ORDER BY error_count DESC LIMIT 10 "
        )
    )
