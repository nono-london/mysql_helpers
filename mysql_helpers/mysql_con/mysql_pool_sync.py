""" Handles queries to MySQL using the mysql-python native connector"""
import logging
from os import environ
from pathlib import Path
from typing import Union, Optional, List, Tuple, Dict

import pandas as pd
from mysql.connector.cursor import MySQLCursor
from mysql.connector.errors import InterfaceError
from mysql.connector.pooling import PooledMySQLConnection, MySQLConnectionPool

logger = logging.getLogger(f"mysql_helpers:{Path(__file__).name}")


class MySQLConnectorPoolNative:
    """MySQL class helpers with Rlock use"""

    def __init__(
            self,
            db_host: Optional[str] = None,
            db_port: Union[int, str] = None,
            db_user: Optional[str] = None,
            db_password: Optional[str] = None,
            db_name: Optional[str] = None,
            raise_on_warnings: bool = False,
            pool_size: int = 30,
            pool_name: Optional[str] = None,
    ):
        self.pool_size: int = min(32, pool_size)  # max siwe is 32
        self.pool_name: Union[str, None] = pool_name

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

        self.mysql_pool: Union[None, MySQLConnectionPool] = self.create_pool()
        self.pool_connections: Dict = (
            {}
        )  # {'a_name_of_conn':Connection1},{'another_name_of_conn':Connection2} ]

    def create_pool(self) -> Union[None, MySQLConnectionPool]:
        """Return mysql connection or None if failure to establish one"""

        try:
            self.mysql_pool = MySQLConnectionPool(
                pool_name=self.pool_name,
                pool_size=self.pool_size,
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                passwd=self.db_password,
                database=self.db_name,
                get_warnings=True,
                raise_on_warnings=self.raise_on_warnings,
            )

            return self.mysql_pool
        except InterfaceError as ex:
            logger.error(
                f"Can't find MySql server on Host: {self.db_host}, Port: {self.db_port}. Error is: {ex}. Error type is: {type(ex)}"
            )

        except Exception as ex:
            print(f"Unhandled,connection error: {ex}")

        return None

    def fetch_all_as_df(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None,
            close_connection: bool = True,
            connection_name: Optional[str] = None,
    ) -> Union[pd.DataFrame, None]:
        """
        :param sql_query: the MySQL query
        :param sql_variables: parameters ordered with %s usage in the sql_query
        :param close_connection: close connection after the method ends
        :return: return a pandas DataFrame if there are results or None if error
        """

        try:
            if (
                    connection_name
                    and self.pool_connections.get(connection_name) is not None
            ):
                conn: PooledMySQLConnection = self.pool_connections[connection_name]
            else:
                conn: PooledMySQLConnection = self.mysql_pool.get_connection()

            mysql_cursor = conn.cursor()
            mysql_cursor.execute(sql_query, sql_variables)
            result_df = pd.DataFrame(mysql_cursor.fetchall())
            result_df.columns = mysql_cursor.column_names
            mysql_cursor.close()

            if close_connection:
                conn.close()
            else:
                self.pool_connections[connection_name] = conn
            return result_df
        except Exception as ex:
            logger.error(
                f"Error while fetching data : {ex}. Exception is {ex.__class__.__name__}"
            )
            return None

    def fetch_all_as_dicts(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None,
            close_connection: bool = True,
            connection_name: Optional[str] = None,
    ) -> Union[List[Tuple], None]:
        """
        :param sql_query: the MySQL query
        :param sql_variables: parameters ordered with %s usage in the sql_query
        :param close_connection: close connection after the method ends
        :return: return a pandas DataFrame if there are results or None if error
        """

        try:
            if (
                    connection_name
                    and self.pool_connections.get(connection_name) is not None
            ):
                conn: PooledMySQLConnection = self.pool_connections[connection_name]
            else:
                conn: PooledMySQLConnection = self.mysql_pool.get_connection()

            mysql_cursor: MySQLCursor = conn.cursor()
            mysql_cursor.execute(sql_query, sql_variables)
            results = mysql_cursor.fetchall()
            mysql_cursor.close()

            if close_connection:
                conn.close()
            else:
                self.pool_connections[connection_name] = conn
            return results

        except Exception as ex:
            logger.error(
                f"Error while inserting new score: {ex}. SQL Statement used: {mysql_cursor.statement}."
            )
            return None

    def execute_one_query(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None,
            close_connection: bool = True,
            connection_name: Optional[str] = None,
    ) -> int:
        """method that handles execute queries: delete update insert
        :param sql_query: the MySQL query
        :param sql_variables: parameters ordered with %s usage in the sql_query
        :param close_connection: close connection after the method ends
        :return: returns the number of rows affected, -1 if connection error ONLY
        """

        rows_affected: int = 0
        try:
            if (
                    connection_name
                    and self.pool_connections.get(connection_name) is not None
            ):
                conn: PooledMySQLConnection = self.pool_connections[connection_name]
            else:
                conn: PooledMySQLConnection = self.mysql_pool.get_connection()

            mysql_cursor: MySQLCursor = conn.cursor()
            mysql_cursor.execute(sql_query, sql_variables)
            rows_affected = mysql_cursor.rowcount
            conn.commit()
            mysql_cursor.close()

            if close_connection:
                conn.close()
            else:
                self.pool_connections[connection_name] = conn

        except Exception as ex:
            logger.error(
                f"Error ({ex.__class__.__name__}) while executing query: {ex}. Variables used: {sql_variables}."
            )
            try:
                logger.error('Statement used: "{mysql_cursor.statement}" ')
            # TODO: use the appropriate Excetion code for undefined variables
            except Exception as ex:
                logger.error(
                    f"Error ({ex.__class__.__name__}) while executing sql statement: {ex}"
                )

        return rows_affected

    def close_connection(self, connection_name: str):
        self.pool_connections[connection_name].close()

    def close_all_connections(self):
        for conn in self.pool_connections:
            self.pool_connections[conn].close()

    #:TODO: add execute many using prepared statement


if __name__ == "__main__":
    from dotenv import load_dotenv
    from mysql_helpers.app_config import logging_config

    logging_config()
    load_dotenv()
    my_getter = MySQLConnectorPoolNative()
    print(
        my_getter.fetch_all_as_dicts(
            sql_query="SELECT * FROM tbl_proxy_url ORDER BY error_count DESC LIMIT 10 "
        )
    )
