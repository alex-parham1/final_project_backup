from dotenv import load_dotenv
import pymysql
import os

target = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# last test for today

load_dotenv(f"{target}/.env")
host = os.environ.get("mysql_host")
user = os.environ.get("mysql_user")
password = os.environ.get("mysql_pass")
port = 3307
db = os.environ.get("mysql_db")
warehouse_db_name = os.environ.get("mysql_db")


def get_connection():
    connection = pymysql.connect(
        host=host, user=user, password=password, database=warehouse_db_name, port=port
    )
    return connection


def close_connection(connection, cursor=None):
    if cursor is not None:
        cursor.close()
    connection.close()


def get_cursor(connection):
    return connection.cursor()


def commit_connection(connection):
    connection.commit()


def execute_cursor(cursor, sql: str):
    if sql == "" or sql == None:
        return
    else:
        cursor.execute(sql)


def get_sqlalchemy():
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"


def commit_and_close(connection, cursor=None):
    commit_connection(connection)
    close_connection(connection, cursor=cursor)
