import threading
import zmq
import os
import time


class broker(threading.Thread):
    def __init__(self) -> None:
        threading.Thread.__init__(self)
        self.ctx = zmq.Context()

    def status(self):
        ret = {"errors": []}
        ctx = zmq.Context()
        pub = ctx.socket(zmq.PUB)
        pub.connect(os.getenv("DATA_PUBLISHER_ADDRESS"))
        sub = ctx.socket(zmq.SUB)
        sub.connect(os.getenv("DATA_SUBSCRIBER_ADDRESS"))
        sub.subscribe("status")

        def heartbeat(event):
            while True:
                if event.is_set():
                    break
                time.sleep(0.1)
                pub.send_string("status:heartbeat")

        event = threading.Event()
        t = threading.Thread(target=heartbeat, args=(event,))
        t.start()

        poller = zmq.Poller()
        poller.register(sub, zmq.POLLIN)
        res = dict(poller.poll(timeout=1000))
        event.set()
        if not sub in res:
            ret["errors"].append("Broker didn't respond to heartbeat within 1 second")
        return ret

    def stop(self):
        self.ctx.term()

    def run(self):
        try:
            publisher = self.ctx.socket(zmq.XPUB)
            publisher.bind(os.getenv("DATA_SUBSCRIBER_ADDRESS"))
            subscriber = self.ctx.socket(zmq.XSUB)
            subscriber.bind(os.getenv("DATA_PUBLISHER_ADDRESS"))

            zmq.proxy(subscriber, publisher)
        except Exception as e:
            pass
