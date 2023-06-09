import threading
import zmq
import os
import time
import socket


class internal_bus(threading.Thread):
    def __init__(self) -> None:
        threading.Thread.__init__(self)
        self.subscriber_addr = os.getenv("INTERNAL_SUBSCRIBER_ADDRESS")
        self.publisher_addr = os.getenv("INTERNAL_PUBLISHER_ADDRESS")
        self.inputs_addr = os.getenv("INPUT_SUBSCRIBER_ADDRESS")
        self.name = "internal_bus"

    def status(self):
        ret = {"errors": []}
        ctx = zmq.Context.instance()
        pub = ctx.socket(zmq.PUB)
        pub.connect(self.publisher_addr)
        sub = ctx.socket(zmq.SUB)
        sub.connect(self.subscriber_addr)
        sub.subscribe("status")

        def heartbeat():
            while True:
                try:
                    time.sleep(0.1)
                    pub.send_string("status:heartbeat")
                except Exception:
                    break

        t = threading.Thread(target=heartbeat)
        t.start()

        poller = zmq.Poller()
        poller.register(sub, zmq.POLLIN)
        res = dict(poller.poll(timeout=1000))
        if not sub in res:
            ret["errors"].append("Broker didn't respond to heartbeat within 1 second")

        sub.close()
        pub.close()
        return ret

    def run(self):
        ctx = zmq.Context.instance()
        publisher = ctx.socket(zmq.PUB)
        publisher.bind(self.subscriber_addr)
        publisher.setsockopt(zmq.LINGER, 0)

        subsciber = ctx.socket(zmq.SUB)
        subsciber.bind(self.publisher_addr)
        subsciber.subscribe("")
        subsciber.setsockopt(zmq.LINGER, 0)

        backend = ctx.socket(zmq.REQ)
        backend.identity = "Internal-Bus-{}".format(socket.gethostname()).encode(
            "ascii"
        )
        backend.connect(self.inputs_addr)
        backend.send(b"READY")
        backend.setsockopt(zmq.LINGER, 0)

        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)
        poller.register(subsciber, zmq.POLLIN)

        while True:
            try:
                sockets = dict(poller.poll(timeout=1000))
                if backend in sockets:
                    request = backend.recv_string()
                    print("{}: {}".format("Internal-Bus", request))
                    publisher.send_string(request)
                    backend.send(b"OK")

                if subsciber in sockets:
                    request = subsciber.recv_string()
                    print("{}: {}".format("Internal-Bus", request))
                    publisher.send_string(request)
            except zmq.ContextTerminated:
                break
            except Exception:
                continue

        publisher.close()
        subsciber.close()
        backend.close()
