# Read data from a file
import os

def process_item(message: str):
    try:
        message = message.split(":")[1]
        # message -> ./app/data/file.txt
        splitted = message.split("/")
        file_name = splitted[len(splitted)-1]
        path = message.replace(file_name, "")

        if file_name not in os.listdir(path):
            raise Exception("File does not exist")

        if os.path.getsize(path + file_name) == 0:
            raise Exception("File Empty")

        text = []
        with open(path + file_name, "r") as file:
            for line in file:
                text.append(line[:-1].split(";"))
            
        return str([text, "database;mydb;table;cars"])
    except Exception as e:
        return str(e)
