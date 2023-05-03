import mysql.connector
import json


def process_item(message: str):
    try:
        # data = message.get_json()
        message = message.replace(message.split(":")[0] + ":", "")
        master = json.loads(message)

        database = master["database"]
        table_name = master["table"]
        data = master["data"]


        mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="secret",
            database=database,
            port=33060
        )

        mycursor = mydb.cursor()
        mycursor.execute("SHOW TABLES")

        table_exists = False

        for table in mycursor.fetchall():
            if table_name in table:
                table_exists = True
                break

        if not table_exists:
            mycursor.execute(f"CREATE TABLE {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, brand VARCHAR(255), model VARCHAR(255))")


        for element in data:
            line = (element["brand"], element["model"])
            query = f"INSERT INTO {table_name} (brand, model) VALUES (%s, %s)"
            mycursor.execute(query, line)

        mydb.commit()

        mycursor.execute(f"SELECT * FROM {table_name}")

        rows = mycursor.fetchall()

        for row in rows:
            print(row)

        mycursor.close()
        mydb.close()


        print(mycursor.rowcount, "record(s) affected")

        return "OK"
    except Exception as e:
        print(str(e))
        return str(e)