import ast

def process_item(message):
    try:
        message = message.split(":")[1]

        number1 = int(ast.literal_eval(message)[0])
        number2 = int(ast.literal_eval(message)[1])

        return str(number1 * number2)
    except Exception as e:
        return str(e)
    