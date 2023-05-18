import logging
import socket


class logger(logging.Handler):
    def __init__(self, sql_conn, sql_cursor):
        logging.Handler.__init__(self)
        self.sql_cursor = sql_cursor
        self.sql_conn = sql_conn

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
