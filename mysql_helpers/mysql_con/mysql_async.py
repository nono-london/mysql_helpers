"""inspired from docs
https://dev.mysql.com/doc/connector-python/en/connector-python-asyncio.html
"""
import asyncio
import logging
from os import environ
from pathlib import Path
from typing import (Union, Optional, Tuple, List)

import pandas as pd
from mysql.connector.aio import MySQLConnectionAbstract as _MySQLConnectionAbstract
from mysql.connector.aio import connect as _connect
from mysql.connector.aio.cursor import MySQLCursorAbstract as _MySQLCursorAbstract

logger = logging.getLogger(f"mysql_helpers:{Path(__file__).name}")


class MySQLConnectorNativeAsync:
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
            int(environ["MYSQL_DB_PORT"]) if db_port is None else int(db_port)
        )
        self.db_user: str = environ["MYSQL_DB_USER"] if db_user is None else db_user
        self.db_password: str = (
            environ["MYSQL_DB_PASS"] if db_password is None else db_password
        )
        self.db_name: str = environ["MYSQL_DB_NAME"] if db_name is None else db_name
        self.raise_on_warnings: bool = raise_on_warnings
        self.mysql_connection: Union[None, _MySQLConnectionAbstract] = None

    async def open_connection(self) -> _MySQLConnectionAbstract:
        """Return mysql connection if needed or raise an error"""
        if self.mysql_connection is not None and self.mysql_connection.is_connected():
            return self.mysql_connection

        try:
            self.mysql_connection = await _connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
                get_warnings=True,
                raise_on_warnings=self.raise_on_warnings,
            )
            return self.mysql_connection
        except Exception as ex:
            raise ConnectionError(f'Failed to connect to database\n'
                                  f'Exception: {ex}')

    async def close_connection(self):
        if self.mysql_connection is not None and await self.mysql_connection.is_connected():
            await self.mysql_connection.close()

    async def fetch_all_as_df(
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

        await self.open_connection()

        result_df = Union[pd.DataFrame, None]
        try:
            mysql_cursor = await self.mysql_connection.cursor()
            await mysql_cursor.execute(sql_query, sql_variables)
            result_df = pd.DataFrame(await mysql_cursor.fetchall())
            result_df.columns = mysql_cursor.column_names
            await mysql_cursor.close()
        except Exception as ex:
            logger.error(
                f"Error while fetching data : {ex}. Exception is {ex.__class__.__name__}"
            )
        finally:
            if close_connection:
                await self.close_connection()

        return result_df

    async def fetch_all_as_dicts(
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
        # open connection if needed
        await self.open_connection()

        mysql_cursor: Union[_MySQLCursorAbstract, None] = None
        results: Union[List[Tuple], None] = None
        try:
            mysql_cursor = await self.mysql_connection.cursor()
            await mysql_cursor.execute(sql_query, sql_variables)
            results = await mysql_cursor.fetchall()
            await mysql_cursor.close()
        except Exception as ex:
            if mysql_cursor:
                logger.error(
                    f"Error while inserting new score: {ex}. SQL Statement used: {mysql_cursor.statement}"
                )
            else:
                logger.error(
                    f"Error while inserting new score: {ex} - "
                    f"SQL statement used: {sql_query} - "
                    f"SQL variables used: {sql_variables} - "
                )
        finally:
            if close_connection:
                await self.close_connection()

        return results

    async def execute_one_query(
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

        await self.open_connection()

        rows_affected: int = 0
        mysql_cursor: Union[_MySQLCursorAbstract, None] = None
        try:
            mysql_cursor = await self.mysql_connection.cursor()
            await mysql_cursor.execute(sql_query, sql_variables)
            rows_affected = mysql_cursor.rowcount
            await self.mysql_connection.commit()
            await mysql_cursor.close()
        except Exception as ex:
            if mysql_cursor:
                logger.error(
                    f"Error while inserting new score: {ex}. SQL Statement used: {mysql_cursor.statement}"
                )
            else:
                logger.error(
                    f"Error while inserting new score: {ex} - "
                    f"SQL statement used: {sql_query} - "
                    f"SQL variables used: {sql_variables} - "
                )
        finally:
            if close_connection:
                await self.close_connection()

        return rows_affected


if __name__ == "__main__":
    from dotenv import load_dotenv
    from mysql_helpers.app_config import logging_config

    load_dotenv()
    logging_config()

    my_getter = MySQLConnectorNativeAsync()
    loop = asyncio.get_event_loop()
    print(
        loop.run_until_complete(my_getter.fetch_all_as_df(
            sql_query="SELECT * FROM tbl_proxy_url ORDER BY error_count DESC LIMIT 10 "
        )
        )
    )
