import mysql.connector

def process_item(message: str):
    try:
        result = message.split(":")[1]


        mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="secret",
            database="mydb",
            port=33060
        )

        mycursor = mydb.cursor()
        mycursor.execute("SHOW TABLES")

        table_exists = False

        for table in mycursor.fetchall():
            if "results" in table:
                table_exists = True
                break

        if not table_exists:
            mycursor.execute("CREATE TABLE results (id INT AUTO_INCREMENT PRIMARY KEY, result VARCHAR(255))")

        tipus = type(result)

        line = (result, )
        query = f"INSERT INTO results (result) VALUES (%s)"
        mycursor.execute(query, line)

        mydb.commit()

        mycursor.execute(f"SELECT * FROM results")

        rows = mycursor.fetchall()

        for row in rows:
            print(row)

        mycursor.close()
        mydb.close()

        print(mycursor.rowcount, "record(s) affected")

        return "OK"
    except Exception as e:
        return str(e)