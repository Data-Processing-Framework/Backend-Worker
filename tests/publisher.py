import zmq
import time


def publisher_thread():
    ctx = zmq.Context.instance()

    publisher = ctx.socket(zmq.PUB)
    publisher.connect("tcp://127.0.0.1:5556")

    while True:
        string = "input:" + str("2")
        try:
            publisher.send(string.encode("utf-8"))
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break  # Interrupted
            else:
                raise
        time.sleep(1)  # Wait for 1/10th second


publisher_thread()
