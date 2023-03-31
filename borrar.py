import zmq
import time
from threading import Thread


def publisher_thread():
    ctx = zmq.Context.instance()

    publisher = ctx.socket(zmq.PUB)
    publisher.connect("tcp://127.0.0.1:5558")

    while True:
        string = "input:" + str("2")
        try:
            publisher.send_string(string)
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break  # Interrupted
            else:
                raise
        time.sleep(1)  # Wait for 1/10th second


for a in range(10):
    Thread(target=publisher_thread).start()
