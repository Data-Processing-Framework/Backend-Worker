import zmq
import os
import threading
import time
import logging
from src.logger import logger


class input(threading.Thread):
    def __init__(self, name: str, process_item: callable, polling_time) -> None:
        threading.Thread.__init__(self)
        self.name = name
        self.process_item = process_item
        self.publisher_addr = os.getenv("DATA_PUBLISHER_ADDRESS")
        self.polling_time = polling_time

    def status(self):
        res = {"errors": []}
        return res

    def run(self):
        context = zmq.Context.instance()
        publisher = context.socket(zmq.PUB)
        publisher.connect(self.publisher_addr)
        master = ""

        logdb = logger()
        log = logging.getLogger(self.name + ";Input")
        log.handlers.clear()
        log.addHandler(logdb)
        log.setLevel(logging.INFO)

        time.sleep(5)
        while True:
            try:
                try:
                    result = self.process_item()
                except Exception as e:
                    log.error(str(e))
                    continue
                log.info(result + " -> OK")
                # if result != master:
                master = result
                publisher.send_string(self.name + ":" + result)
                time.sleep(self.polling_time)
            except zmq.ContextTerminated:
                break
            except Exception:
                continue

        publisher.close()
