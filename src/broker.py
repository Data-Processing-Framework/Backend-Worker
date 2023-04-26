import threading
import zmq
import os


class broker(threading.Thread):
    def run(self):
        ctx = zmq.Context()
        publisher = ctx.socket(zmq.XPUB)
        publisher.bind(os.getenv("DATA_SUBSCRIBER_ADDRESS"))
        subscriber = ctx.socket(zmq.XSUB)
        subscriber.bind(os.getenv("DATA_PUBLISHER_ADDRESS"))

        zmq.proxy(subscriber, publisher)
