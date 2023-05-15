import logging
import pymssql
import time


class logger(logging.Handler):
    """
    Customized logging handler that puts logs to the database.
    pymssql required
    """

    def __init__(self, sql_conn, sql_cursor, db_tbl_log):
        logging.Handler.__init__(self)
        self.sql_cursor = sql_cursor
        self.sql_conn = sql_conn
        self.db_tbl_log = db_tbl_log

    def emit(self, record):
        # Set current time
        tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))
        # Clear the log message so it can be put to db via sql (escape quotes)
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace("'", "''")
        # Make the SQL insert
        sql = (
            "INSERT INTO "
            + self.db_tbl_log
            + " (log_level, "
            + "log_levelname, log, created_at, created_by) "
            + "VALUES ("
            + ""
            + str(record.levelno)
            + ", "
            + "'"
            + str(record.levelname)
            + "', "
            + "'"
            + str(self.log_msg)
            + "', "
            + "(convert(datetime2(7), '"
            + tm
            + "')), "
            + "'"
            + str(record.name)
            + "')"
        )
        try:
            self.sql_cursor.execute(sql)
            self.sql_conn.commit()
        # If error - print it out on screen. Since DB is not working - there's
        # no point making a log about it to the database :)
        except pymssql.Error as e:
            print(sql)
            print("CRITICAL DB ERROR! Logging to database not possible!")
