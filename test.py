import time
from threading import Thread
from transform import transform
import zmq


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
            publisher.send(string.encode("utf-8"))
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break  # Interrupted
            else:
                raise
        time.sleep(0.1)  # Wait for 1/10th second


tr1 = transform("transform1", ["input"], process_item, 10)
# tr1.run()
Thread(target=tr1.run).start()
tr2 = transform("transform2", ["transform1"], process_item, 10)
Thread(target=tr2.run).start()


publisher_thread()
