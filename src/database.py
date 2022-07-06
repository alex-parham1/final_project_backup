from dotenv import load_dotenv
import pymysql
import os

load_dotenv()
host = os.environ.get("mysql_host")
user = os.environ.get("mysql_user")
password = os.environ.get("mysql_pass")
warehouse_db_name = os.environ.get("mysql_db")


def get_connection():
    connection = pymysql.connect(
        host=host, user=user, password=password, database=warehouse_db_name
    )
    return connection


def close_connection(connection, cursor=None):
    if cursor is not None:
        cursor.close()
    connection.close()


def commit_connection(connection):
    connection.commit()