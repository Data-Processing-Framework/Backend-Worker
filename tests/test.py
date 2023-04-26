from dotenv import load_dotenv
import sys
from os import path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from src.broker import broker
from src.modules.input import input
from src.modules.transform import transform
import zmq
from threading import Thread
import time


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
        string = "input:" + str("1")
        try:
            publisher.send_string(string)
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break  # Interrupted
            else:
                raise
        time.sleep(0.1)  # Wait for 1/10th second


in1 = input("input1", dummy_input, 0.1)
tr1 = transform("transform1", ["input1"], process_item, 10)
br=broker()
tr2 = transform("transform2", ["transform1"], process_item, 10)
br.start()
in1.start()
tr1.start()
tr2.start()


publisher_thread()
