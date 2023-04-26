import time
from threading import Thread
import zmq

import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from src.modules.transform import transform
from src.modules.output import output
from src.data.modules import insertDB
from src import broker
from dotenv import load_dotenv

load_dotenv()


def process_item(message: str):
    time.sleep(1)
    return message


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


tr1 = transform("transform1", ["input"], process_item, 10)
tr1.start()
tr2 = transform("transform2", ["transform1"], process_item, 10)
tr2.start()
# Thread(target=broker.run).start()

tr3 = output("outpu1", ["transform1"], insertDB.process_item, 10)
tr3.start()
tr4 = output("outpu2", ["transform2"], process_item, 10)
tr4.start()
Thread(target=broker.run).start()


publisher_thread()
