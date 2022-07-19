from mysql_helpers.native_conn.mysql_handler import MySQLConnectorLib


def test_select_query():
    my_getter = MySQLConnectorLib()
    mysql_query = """
    SELECT table_name FROM information_schema.tables;
    """
    print(my_getter.select_query(sql_query=mysql_query,
                                 as_df=True))
    print(my_getter.select_query(sql_query=mysql_query,
                                 as_df=False))


if __name__ == '__main__':
    test_select_query()
