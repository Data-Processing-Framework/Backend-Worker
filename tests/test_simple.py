import time
from threading import Thread
import zmq
from dotenv import load_dotenv
import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.modules.input import input
from src.modules.transform import transform
from src.internal_bus import internal_bus
from src.modules.output import output
from src.broker import broker
from dotenv import load_dotenv

load_dotenv()


def dummy_input():
    # print("input-funciona!")
    return "dummy"


def dummy_transform(message):
    # print("transform-" + message)
    return message


br = broker()
br.start()
time.sleep(1)
print(br.status())

ib = internal_bus()
ib.start()
time.sleep(1)
print(ib.status())

in1 = input("input1", dummy_input, 1)
in1.start()

tr1 = transform("transform1", ["input1"], dummy_transform, 10)
tr1.start()

tr2 = transform("transform2", ["transform1", "input1"], dummy_transform, 1)
tr2.start()

output1 = output("output1", ["transform1", "transform2"], dummy_transform, 1)
output1.start()
