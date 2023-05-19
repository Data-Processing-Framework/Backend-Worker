import logging
import socket
import mysql.connector
import os


class logger(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self)
        db_server = os.getenv("LOGGING_DB_ADDRESS")
        db_user = os.getenv("LOGGING_DB_USER")
        db_password = os.getenv("LOGGING_DB_PASSWORD")
        db_dbname = os.getenv("LOGGING_DB_NAME")

        self.sql_conn = mysql.connector.connect(
            host=db_server,
            user=db_user,
            password=db_password,
            database=db_dbname,
        )
        self.sql_cursor = self.sql_conn.cursor()

    def __del__(self):
        self.sql_cursor.close()
        self.sql_conn.close()

    def emit(self, record):
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace("'", "''")

        sql = f"INSERT INTO Workers (log_level, log_levelname, log, created_by, type, name) VALUES ({record.levelno}, '{record.levelname}', '{self.log_msg}', '{socket.gethostname()}', '{record.name.split(';')[1]}', '{record.name.split(';')[0]}')"
        try:
            self.sql_cursor.execute(sql)
            self.sql_conn.commit()

        except Exception as e:
            print(sql)
            print("CRITICAL DB ERROR! Logging to database not possible!")
