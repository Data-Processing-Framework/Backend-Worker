import zmq
import os
import threading
import time


class input(threading.Thread):
    def __init__(
        self, name: str, process_item: callable, polling_time
    ) -> None:
        threading.Thread.__init__(self)
        self.name = name
        self.process_item = process_item
        self.publisher_addr = os.getenv("DATA_PUBLISHER_ADDRESS")
        self.stopper = False
        self.polling_time = polling_time

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
        publisher = context.socket(zmq.PUB)
        publisher.connect(self.publisher_addr)
        master = ""

        time.sleep(5)
        while not self.stopper:
            try:
                result = self.process_item()
                if result != master:
                    master = result
                    publisher.send_string(self.name + ":" + result)
                time.sleep(self.polling_time)
            except zmq.ContextTerminated:
                continue
        publisher.close()
