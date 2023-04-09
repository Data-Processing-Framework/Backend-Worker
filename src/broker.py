import zmq
import os


def run():
    ctx = zmq.Context()
    publisher = ctx.socket(zmq.XPUB)
    publisher.bind("tcp://127.0.0.1:" + os.getenv("DATA_SUBSCRIBER_PORT"))
    subscriber = ctx.socket(zmq.XSUB)
    subscriber.bind("tcp://127.0.0.1:" + os.getenv("DATA_PUBLISHER_PORT"))

    zmq.proxy(subscriber, publisher)
