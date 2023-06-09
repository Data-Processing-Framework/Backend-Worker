import threading
import zmq
import os


class broker(threading.Thread):
    def __init__(self) -> None:
        threading.Thread.__init__(self)
        self.name = "broker"

    def status(self):
        ret = {"errors": []}
        ctx = zmq.Context.instance()

        backend = ctx.socket(zmq.REQ)
        backend.identity = "heartbeat".encode("ascii")
        backend.connect(os.getenv("INPUT_SUBSCRIBER_ADDRESS"))
        backend.send(b"READY")

        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)
        res = dict(poller.poll(timeout=5000))
        if not backend in res:
            ret["errors"].append("Broker didn't respond to heartbeat within 5 seconds")

        backend.close()
        return ret

    def run(self):
        ctx = zmq.Context.instance()
        subscriber = ctx.socket(zmq.SUB)
        subscriber.bind(os.getenv("DATA_PUBLISHER_ADDRESS"))
        subscriber.subscribe("")
        subscriber.setsockopt(zmq.LINGER, 0)

        backend = ctx.socket(zmq.ROUTER)
        backend.bind(os.getenv("INPUT_SUBSCRIBER_ADDRESS"))
        backend.setsockopt(zmq.LINGER, 0)

        backend_ready = False
        workers = []
        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)

        while True:
            try:
                sockets = dict(poller.poll(timeout=1000))

                if backend in sockets:
                    worker = backend.recv_multipart()[0]
                    if worker == b"heartbeat":
                        backend.send_multipart([worker, b"", b"OK"])
                        continue
                    workers.append(worker)
                    if workers and not backend_ready:
                        poller.register(subscriber, zmq.POLLIN)
                        backend_ready = True

                if subscriber in sockets:
                    request = subscriber.recv()
                    worker = workers.pop(0)
                    backend.send_multipart([worker, b"", request])
                    if not workers:
                        poller.unregister(subscriber)
                        backend_ready = False
            except zmq.ContextTerminated:
                break
            except Exception:
                continue

        backend.close()
        subscriber.close()
