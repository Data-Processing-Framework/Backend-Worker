import mysql.connector


def process_item(message):
    try:
        data = message.get_json()

        mydb = mysql.connector.connect(
            host=data["host"],
            user=data["username"],
            password=["password"],
            database=["database"]
        )

        mycursor = mydb.cursor()

        sql = data["query"]

        mycursor.execute(sql)

        mydb.commit()

        print(mycursor.rowcount, "record(s) affected")

        return "OK"
    except Exception as e:
        return str(e)