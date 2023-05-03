import multiprocessing
import zmq
import os
import threading
from ctypes import c_bool
import queue
import logging

class output(threading.Thread):
    def __init__(
        self, name: str, sub_topics: list, process_item: callable, n_workers: int
    ) -> None:
        threading.Thread.__init__(self)
        self.name = name
        self.sub_topics = sub_topics
        self.process_item = process_item
        self.n_workers = n_workers
        self.timeout = int(os.getenv("WORKER_TIMEOUT"))
        self.subscriber_port = os.getenv("DATA_SUBSCRIBER_PORT")
        self.workers = queue.Queue()
        logging.basicConfig(filename=self.name + ".log", level=logging.DEBUG)

    def worker(self, id, stopper):
        context = zmq.Context.instance()

        backend = context.socket(zmq.REQ)
        backend.identity = "Worker-{}-{}".format(self.name, id).encode("ascii")
        backend.connect("ipc://{}-backend.ipc".format(self.name))
        backend.send(b"READY")

        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)

        while not stopper.value:
            sockets = dict(poller.poll(timeout=self.timeout))
            if backend in sockets:
                request = backend.recv_string()
                print("{}: {}".format(backend.identity.decode("ascii"), request))
                result = self.process_item(request)
                if result is not None and type(result) == str:
                    logging.debug(result)
                backend.send(b"OK")
        backend.close()
        context.term()

    def status(self):
        res = {"errors": []}
        id = 0
        workers = list(self.workers.queue)
        for w in workers:
            res["Worker-{}-{}".format(self.name, id)] = w.is_alive()
            id += 1
        if self.n_workers != len(self.workers.queue):
            res["errors"].append(
                "Expected {} workers but got {} workers".format(
                    self.n_workers, len(self.workers.queue)
                )
            )
        return res

    def run(self):
        context = zmq.Context.instance()

        subscriber = context.socket(zmq.SUB)
        subscriber.connect("tcp://127.0.0.1:" + self.subscriber_port)

        stopper = multiprocessing.Value(c_bool, False)

        def start(task, *args):
            process = multiprocessing.Process(target=task, args=args)
            process.daemon = True
            process.start()
            self.workers.put(process)

        for i in range(self.n_workers):
            start(self.worker, i, stopper)

        for topic in self.sub_topics:
            subscriber.setsockopt_string(zmq.SUBSCRIBE, topic)

        backend = context.socket(zmq.ROUTER)
        backend.bind("ipc://{}-backend.ipc".format(self.name))

        backend_ready = False
        workers = []
        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)

        while True:
            try:
                sockets = dict(poller.poll(timeout=self.timeout))

                if backend in sockets:
                    worker = backend.recv_multipart()[0]
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
        stopper.value = True
        while not self.workers.empty:
            w = self.workers.get()
            w.join()
        backend.close()
        subscriber.close()
