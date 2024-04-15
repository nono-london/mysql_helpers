from dotenv import load_dotenv

from mysql_helpers.mysql_con.mysql_pool_sync import MySQLConnectorPoolNative




def test_fetch_as_def():
    load_dotenv()
    my_getter = MySQLConnectorPoolNative(pool_size=2)
    mysql_query = """
    SELECT @@version;
    """
    results = my_getter.fetch_all_as_df(sql_query=mysql_query, close_connection=True)
    assert len(results) > 0


def test_fetch_as_dicts():
    load_dotenv()
    my_getter = MySQLConnectorPoolNative(pool_size=2)
    mysql_query = """
    SELECT @@version;
    """

    results = my_getter.fetch_all_as_dicts(sql_query=mysql_query, close_connection=True)
    assert len(results) > 0


if __name__ == "__main__":
    test_fetch_as_def()
    test_fetch_as_dicts()
