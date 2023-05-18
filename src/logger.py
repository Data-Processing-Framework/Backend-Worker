import logging
import socket
import mysql.connector


class logger(logging.Handler):
    def __init__(self, db_server, db_user, db_password, db_dbname):
        logging.Handler.__init__(self)
        self.db_server = db_server
        self.db_user = db_user
        self.db_password = db_password
        self.db_dbname = db_dbname

    def emit(self, record):
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace("'", "''")

        sql = f"INSERT INTO Workers (log_level, log_levelname, log, created_by, type, name) VALUES ({record.levelno}, '{record.levelname}', '{self.log_msg}', '{socket.gethostname()}', '{record.name.split(';')[1]}', '{record.name.split(';')[0]}')"
        try:
            sql_conn = mysql.connector.connect(
                host=self.db_server,
                user=self.db_user,
                password=self.db_password,
                database=self.db_dbname,
            )

            sql_cursor = sql_conn.cursor()
            sql_cursor.execute(sql)
            sql_conn.commit()
            sql_conn.close()
            sql_cursor.close()

        except Exception as e:
            print(sql)
            print("CRITICAL DB ERROR! Logging to database not possible!")
