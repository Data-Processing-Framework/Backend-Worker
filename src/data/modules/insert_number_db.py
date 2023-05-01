import psycopg2

def process_item(message: str):
    try:

        result = message.split(":")[1]

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="mydb",
            user="postgres",
            password="mysecretpassword"
        )

        cur = conn.cursor()

        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

        table_exists = False

        for table in cur.fetchall():
            if "results" in table:
                table_exists = True
                break

        if not table_exists:
            cur.execute(f"CREATE TABLE results (id SERIAL PRIMARY KEY, result VARCHAR(50))")

        
        line = (result, )
        query = f"INSERT INTO results (result) VALUES (%s)"
        cur.execute(query, line)
        
        conn.commit()

        cur.execute(f"SELECT * FROM results")

        rows = cur.fetchall()

        for row in rows:
            print(row)

        cur.close()
        conn.close()

        return "OK"

    except Exception as e:
        return str(e)
