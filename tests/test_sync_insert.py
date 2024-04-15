""" creates a table names test_help_01, insert rows and delete the table """
from random import randint

from dotenv import load_dotenv

from mysql_helpers.mysql_con.mysql_sync import MySQLConnectorNative

TEST_TABLE_NAME: str = "pytest_temp_1"


def test_insert_in_temp_table():
    load_dotenv()

    # create temp table
    table_upper = MySQLConnectorNative()
    sql_query: str = f"""
            CREATE TEMPORARY TABLE `{TEST_TABLE_NAME}` (
                `proxy_id` int NOT NULL AUTO_INCREMENT,
                `updatetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                `upload_datetime` timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                `proxy_url` varchar(150) NOT NULL,
                `proxy_port` varchar(5) NOT NULL,
                
                `proxy_country` varchar(150) DEFAULT NULL,
                `proxy_town` varchar(245) DEFAULT NULL,
                `proxy_speed` int DEFAULT NULL,
                `error_count` smallint NOT NULL DEFAULT '0',
                `captcha_detected` enum('True','False') NOT NULL DEFAULT 'False',
                `proxy_web_name` varchar(45) DEFAULT NULL,
                `selenium_success` int NOT NULL DEFAULT '0',
            PRIMARY KEY (`proxy_id`),
            UNIQUE KEY `avoid_duplicate` (`proxy_url`,`proxy_port`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            
            """

    result = table_upper.execute_one_query(sql_query=sql_query, close_connection=False)
    print(result)

    # create records
    sql_query = f""" INSERT INTO {TEST_TABLE_NAME} (proxy_url, proxy_port)
                    VALUES (%s, %s)
                """
    nbr_records: int = 50
    for n in range(nbr_records):
        sql_variables = (f"https:\\www.example{n}.com", str(randint(1, 5000)))
        table_upper.execute_one_query(
            sql_query=sql_query, sql_variables=sql_variables, close_connection=False
        )
    # check numbe rof records inserted
    sql_query = f""" SELECT COUNT(*) 
                FROM {TEST_TABLE_NAME}
                """
    result = table_upper.fetch_all_as_dicts(sql_query=sql_query, close_connection=True)

    assert result[0][0] == nbr_records


if __name__ == "__main__":
    test_insert_in_temp_table()
