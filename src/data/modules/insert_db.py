import psycopg2 # pip install psycopg2-binary
import json

def process_item(message: str):
    try:
        message = message.replace(message.split(":")[0] + ":", "")
        master = json.loads(message)

        database = master["database"]
        table_name = master["table"]
        data = master["data"]

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database=database,
            user="postgres",
            password="mysecretpassword"
        )

        cur = conn.cursor()

        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

        table_exists = False

        for table in cur.fetchall():
            if table_name in table:
                table_exists = True
                break

        if not table_exists:
            cur.execute(f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, brand VARCHAR(50), model VARCHAR(50))")

        for element in data:
            line = (element["brand"], element["model"])
            query = f"INSERT INTO {table_name} (brand, model) VALUES (%s, %s)"
            cur.execute(query, line)
        
        conn.commit()

        cur.execute(f"SELECT * FROM {table_name}")

        rows = cur.fetchall()

        for row in rows:
            print(row)

        cur.close()
        conn.close()

        return "OK"

    except Exception as e:
        return str(e)
