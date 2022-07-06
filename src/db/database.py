import dotenv
import pymysql
import os


dotenv.load_dotenv()
host = os.environ.get("mysql_host")
user = os.environ.get("mysql_user")
password = os.environ.get("mysql_pass")
warehouse_db_name = os.environ.get("mysql_db")


def get_connection():
    connection = pymysql.connect(
    host=host,
    user=user,
    password=password,
    database=warehouse_db_name
    )
    return connection


def close_connection (connection):
    connection.close()


def commit_connection (connection):
    connection.commit()
