import time
from threading import Thread
import zmq
from dotenv import load_dotenv
import sys
from os import path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.modules.input import input
from src.modules.transform import transform
from src.modules.output import output
from src.data.modules import read_data
from src.data.modules import transform_data
from src.data.modules import insert_db
from src.data.modules import insert_db_mysql
from src.data.modules import read_numbers
from src.data.modules import multiplication
from src.data.modules import insert_number_db
from src.data.modules import insert_number_db_mysql
from src.broker import broker
from dotenv import load_dotenv

load_dotenv()


def process_item(message: str):
    time.sleep(1)
    return message


def dummy_input():
    return "dummy"


def publisher_thread():
    ctx = zmq.Context.instance()

    publisher = ctx.socket(zmq.PUB)
    publisher.connect("tcp://127.0.0.1:5556")

    while True:
        string = "input:" + str("./src/data/modules/file1.txt")
        string3 = "input3:" + str("./src/data/modules/file2.csv")
        try:
            publisher.send_string(string)
            publisher.send_string(string3)
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break  # Interrupted
            else:
                raise
        time.sleep(0.1)  # Wait for 1/10th second



in1 = input("input1", read_data.process_item, 1)
in1.start()
in2 = input("input2", read_numbers.process_item, 10)
in2.start()

tr1 = transform("transform1", ["input1"], transform_data.process_item, 1)
tr1.start()
tr2 = transform("transform2", ["input2"], multiplication.process_item, 10)
tr2.start()

ou1 = output("outpu1", ["transform1"], insert_db.process_item, 1)
ou1.start()
ou2 = output("outpu2", ["transform1"], insert_db_mysql.process_item, 1)
ou2.start()
ou1 = output("outpu3", ["transform2"], insert_number_db.process_item, 1)
ou1.start()
ou2 = output("outpu4", ["transform2"], insert_number_db_mysql.process_item, 1)
ou2.start()

br=broker()

br.start()
