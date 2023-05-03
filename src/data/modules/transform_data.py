# Pass data to json
import json
import ast

def process_item(message: str):
    try:
        print(message)
        message = message.split(":")[1]
        # message -> [[['brand', 'model'], ['Tesla', 'ModelS'], ['Nissan', 'Terrano'], ['Citroen', 'Xantia'], ['Audi', 'Q7']], 'database;mydb;table;cars']
        data = {}
        master = {}
        keys = []

        message = ast.literal_eval(message)

        for key in message[0][0]:
            keys.append(key)

        # message[1] -> database;mydb;table;people
        master["database"] = message[1].split(";")[1]
        master["table"] = message[1].split(";")[3]
        master["data"] = []

        for line in message[0][1:]:
            data = {}
            for i in range(0, len(keys)):
                data[keys[i]] = line[i]
            master["data"].append(data)
        
        json_object = json.dumps(master, indent = 4)
            
        return str(json_object)
    except Exception as e:
        return str(e)
