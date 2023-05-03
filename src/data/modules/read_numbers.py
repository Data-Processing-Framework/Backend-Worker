# Read 2 numbers form csv file
import os

def process_item():
    try:
        # message = message.split(":")[1]
        message = "./src/data/modules/file2.csv"
        # message -> ./app/data/file.txt
        splitted = message.split("/")
        print(splitted)
        file_name = splitted[len(splitted)-1]
        path = message.replace(file_name, "")

        if file_name not in os.listdir(path):
            raise Exception("File does not exist")

        if os.path.getsize(path + file_name) == 0:
            raise Exception("File Empty")

        text = []
        with open(path + file_name, "r") as file:
            for line in file:
                text.append(line.split(","))

        text = text[1]
            
        return str(text)
    except Exception as e:
        return str(e)
