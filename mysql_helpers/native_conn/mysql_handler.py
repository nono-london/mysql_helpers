""" Handles queries to MySQL """
from threading import RLock
from typing import Union

import pandas as pd
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.errors import InterfaceError

from mysql_helpers.app_config_secret import (MYSQL_USER_NAME, MYSQL_PASSWORD, MYSQL_DATABASE_NAME,
                                             MYSQL_PORT, MYSQL_URL)


class MySQLConnectorLib:
    """ MySQL class helpers with Rlock use  """

    def __init__(self, mysql_url: str = MYSQL_URL, mysql_port: Union[int, str] = MYSQL_PORT,
                 mysql_database_name: str = MYSQL_DATABASE_NAME,
                 proxy_universe_size: int = 100):

        self.MYSQL_URL: str = mysql_url
        self.MYSQL_DATABASE_NAME: str = mysql_database_name
        self.MYSQL_PORT: str = str(mysql_port)

        self.proxy_universe_size: int = proxy_universe_size
        self.mysql_connection: Union[None, MySQLConnection] = None

        self.mysql_connection_rlock: RLock = RLock()

    def _get_connection(self) -> Union[None, MySQLConnection]:
        """Return mysql connection or None if failure to establish one"""
        if self.mysql_connection is not None and self.mysql_connection.is_connected():
            return self.mysql_connection

        try:
            self.mysql_connection = MySQLConnection(host=self.MYSQL_URL,
                                                    port=self.MYSQL_PORT,
                                                    user=MYSQL_USER_NAME,
                                                    passwd=MYSQL_PASSWORD,
                                                    database=self.MYSQL_DATABASE_NAME,
                                                    get_warnings=True,
                                                    raise_on_warnings=True,
                                                    )
            return self.mysql_connection
        except InterfaceError as ex:
            print(
                f"Can't find MySql server on Host: {self.MYSQL_URL}, Port: {self.MYSQL_PORT}.\n"
                f"Error is: {ex}\n"
                f"Error type is: {type(ex)}")
            return None

    def _close_connection(self):
        if self.mysql_connection is not None and self.mysql_connection.is_connected():
            self.mysql_connection.close()

    def select_query(self, sql_query: str,
                     sql_variables: tuple = None,
                     as_df: bool = True) -> Union[None, pd.DataFrame, list]:
        """ return select query as list or pandas DataFrame or None if error"""

        self.mysql_connection_rlock.acquire()
        self._get_connection()

        mysql_cursor: MySQLCursor = self.mysql_connection.cursor()
        try:
            mysql_cursor.execute(sql_query, sql_variables)
            if as_df:
                result_df = pd.DataFrame(mysql_cursor.fetchall())
                result_df.columns = mysql_cursor.column_names
                return result_df
            else:
                return mysql_cursor.fetchall()

        except Exception as ex:
            print('Error while inserting new score :', ex)
            print(f'SQL Statement used: "{mysql_cursor.statement}" ', )
            return None
        finally:
            mysql_cursor.close()
            self._close_connection()
            self.mysql_connection_rlock.release()

    def execute_no_return_query(self, sql_query: str, sql_variables: tuple = None):
        """ method that handles execute queries with no return: delete update insert"""
        self.mysql_connection_rlock.acquire()
        self._get_connection()

        mysql_cursor: MySQLCursor = self.mysql_connection.cursor()
        rows_affected = -1
        try:
            mysql_cursor.execute(sql_query, sql_variables)
            rows_affected = mysql_cursor.rowcount
            self.mysql_connection.commit()
            mysql_cursor.close()
            self._close_connection()
        except Exception as ex:
            print(f'Error while executing query : {ex}')
            print(f'SQL statement used: "{mysql_cursor.statement}" ', )
            return rows_affected
        finally:
            self.mysql_connection_rlock.release()
        return rows_affected


if __name__ == '__main__':
    my_getter = MySQLConnectorLib()
    print(my_getter.select_query(sql_query="SELECT * FROM tbl_proxy_url ORDER BY error_count DESC LIMIT 10 "))
