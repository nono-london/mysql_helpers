import asyncio

from dotenv import load_dotenv

from mysql_helpers.mysql_con.mysql_async import MySQLConnectorNativeAsync


def test_mysql_async_fetch_as_dict():
    load_dotenv()

    my_getter = MySQLConnectorNativeAsync()

    loop = asyncio.get_event_loop()
    sql_query = """SELECT @@version"""
    results = loop.run_until_complete(my_getter.fetch_all_as_df(sql_query=sql_query))

    assert len(results) > 0


def test_mysql_async_fetch_as_df():
    load_dotenv()

    my_getter = MySQLConnectorNativeAsync()

    loop = asyncio.get_event_loop()
    sql_query = """SELECT @@version"""
    result_df = loop.run_until_complete(my_getter.fetch_all_as_df(sql_query=sql_query))

    assert len(result_df) > 0


if __name__ == '__main__':
    test_mysql_async_fetch_as_dict()
    test_mysql_async_fetch_as_df()
