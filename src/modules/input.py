import zmq
import os
import threading
import time
import logging


class input(threading.Thread):
    def __init__(self, name: str, process_item: callable, polling_time) -> None:
        threading.Thread.__init__(self)
        self.name = name
        self.process_item = process_item
        self.publisher_addr = os.getenv("DATA_PUBLISHER_ADDRESS")
        self.polling_time = polling_time
        self.logger = logging.getLogger(self.name)

    def status(self):
        res = {"errors": []}
        return res

    def run(self):
        context = zmq.Context.instance()
        publisher = context.socket(zmq.PUB)
        publisher.connect(self.publisher_addr)
        master = ""

        time.sleep(5)
        while True:
            try:
                result = self.process_item()
                self.logger.info(result)
                # if result != master:
                master = result
                publisher.send_string(self.name + ":" + result)
                time.sleep(self.polling_time)
            except zmq.ContextTerminated:
                break
            except Exception:
                continue

        publisher.close()
