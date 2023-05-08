import sys
import os
from dotenv import load_dotenv
import zmq
import time


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()
from src.broker import broker
from src.internal_bus import internal_bus

broker = broker()
broker.start()

internal_bus = internal_bus()
internal_bus.start()

ctx = zmq.Context()
pub = ctx.socket(zmq.PUB)
pub.connect(os.getenv("DATA_PUBLISHER_ADDRESS"))

while True:
    pub.send_string("status:heartbeat")
    time.sleep(1)
