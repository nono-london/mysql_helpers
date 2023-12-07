from dotenv import load_dotenv

from mysql_helpers.mysql_con.mysql_sync import MySQLConnectorNative

load_dotenv()


def test_fetch_as_def():
    my_getter = MySQLConnectorNative()
    mysql_query = """
    SELECT table_name FROM information_schema.tables;
    """
    results = my_getter.fetch_all_as_df(sql_query=mysql_query,
                                        close_connection=True)
    assert len(results) > 0


def test_fetch_as_dicts():
    my_getter = MySQLConnectorNative()
    mysql_query = """
    SELECT table_name FROM information_schema.tables;
    """

    results = my_getter.fetch_all_as_dicts(sql_query=mysql_query,
                                           close_connection=True)
    assert len(results) > 0


if __name__ == '__main__':
    test_fetch_as_def()
    test_fetch_as_dicts()
