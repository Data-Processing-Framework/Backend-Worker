import json


def process_item(message):
    data = message.get_json()

    number1 = int(data["number1"])
    
    number2 = int(data["number2"])

    return str(number1 * number2)