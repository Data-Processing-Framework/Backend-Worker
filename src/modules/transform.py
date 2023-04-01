import multiprocessing
import zmq


class transform:
    def __init__(
        self,
        name: str,
        sub_topics: list,
        process_item: callable,
        n_workers: int,
        description: str,
        type_in: str,
        type_out: str,
    ) -> None:
        self.name = name
        self.sub_topics = sub_topics
        self.process_item = process_item
        self.n_workers = n_workers
        self.description = description
        self.type_out = type_out
        self.type_in = type_in

    def worker(self, id):
        ctx = zmq.Context.instance()
        publisher = ctx.socket(zmq.PUB)
        publisher.connect("tcp://127.0.0.1:5556".format(self.name))

        socket = zmq.Context().socket(zmq.REQ)
        socket.identity = "Worker-{}-{}".format(self.name, id).encode("ascii")
        socket.connect("ipc://{}-backend.ipc".format(self.name))
        socket.send(b"READY")

        while True:
            request = socket.recv_string()
            print("{}: {}".format(socket.identity.decode("ascii"), request))
            result = self.process_item(request)
            publisher.send_string(self.name + ":" + result)
            socket.send(b"OK")

    def run(self):
        context = zmq.Context.instance()

        subscriber = context.socket(zmq.SUB)
        subscriber.connect("tcp://127.0.0.1:5555")

        def start(task, *args):
            process = multiprocessing.Process(target=task, args=args)
            process.daemon = True
            process.start()

        for i in range(self.n_workers):
            start(self.worker, i)

        for topic in self.sub_topics:
            subscriber.setsockopt_string(zmq.SUBSCRIBE, topic)

        backend = context.socket(zmq.ROUTER)
        backend.bind("ipc://{}-backend.ipc".format(self.name))

        backend_ready = False
        workers = []
        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)

        while True:
            sockets = dict(poller.poll())

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
