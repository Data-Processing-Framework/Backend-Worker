import time


def process_item(message: str):
    time.sleep(1)
    print(message)
    return message
