import zmq


def run():
    ctx = zmq.Context.instance()
    publisher = ctx.socket(zmq.XPUB)
    publisher.bind("tcp://127.0.0.1:5555")
    subscriber = ctx.socket(zmq.XSUB)
    subscriber.bind("tcp://127.0.0.1:5556")

    zmq.proxy(subscriber, publisher)
