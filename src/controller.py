import zmq
import os
import json
from importlib import import_module
from src.modules.transform import transform
from src.modules.input import input
from src.broker import broker
from src.internal_bus import internal_bus
import threading
from src.logger import logger
import pymssql
import logging


class controller:
    def __init__(self) -> None:
        context = zmq.Context()
        self.subscriber = context.socket(zmq.SUB)
        self.subscriber.connect(os.getenv("CONTROLLER_REQUEST_ADDRESS"))
        self.subscriber.subscribe("")
        self.response = context.socket(zmq.PUB)
        self.response.connect(os.getenv("CONTROLLER_RESPONSE_ADDRESS"))
        self.isInput = int(os.getenv("IS_INPUT"))
        self.isRestarting = threading.Event()
        self.isStopped = threading.Event()
        self.isStopped.set()
        self.isStopping = threading.Event()
        self.init_logger()

        self.nodes = []
        self.n_nodes = 0
        res = self.update_graph()
        if res != "OK":
            raise Exception(res)

    def init_logger(self):
        db_server = os.getenv("LOGGING_DB_ADDRESS")
        db_user = os.getenv("LOGGING_DB_USER")
        db_password = os.getenv("LOGGING_DB_PASSWORD")
        db_dbname = os.getenv("LOGGING_DB_NAME")
        db_tbl_log = "Input Worker" if self.isInput else "Worker"
        log_conn = pymssql.connect(db_server, db_user, db_password, db_dbname, 30)
        log_cursor = log_conn.cursor()
        logdb = logger(log_conn, log_cursor, db_tbl_log)
        logging.getLogger("").addHandler(logdb)

    def create_node(self, node):
        module_path = "./src/data/modules/{}.py".format(node["module"])
        if not os.path.isfile(module_path):
            raise Exception("Module {} not found".format(node["module"]))
        module = import_module("src.data.modules." + node["module"])

        match (node["type"]):
            case "Input":
                return input(node["name"], module.process_item, 1)
            case "Transform":
                return transform(node["name"], node["inputs"], module.process_item, 1)
            case "Output":
                pass
            case _:
                pass
        return "OK"

    def update_graph(self):
        if not self.isStopped.is_set() or self.isStopping.is_set():
            return "System is running"

        graph_file = open("./src/data/graph.json", "r")
        graph = json.load(graph_file)
        if self.isInput:
            b = broker()
            self.nodes.append(b)
            b.start()
            self.graph = [g for g in graph if g["type"] == "Input"]
        else:
            self.graph = [g for g in graph if g["type"] != "Input"]
            ib = internal_bus()
            self.nodes.append(ib)
            ib.start()

        for a in self.graph:
            node = self.create_node(a)
            if node != "OK":
                self.nodes.append(node)
                node.start()
        self.isStopped.clear()
        return "OK"

    def stop(self):
        if self.isStopped.is_set() or self.isStopping.is_set():
            return
        self.isStopping.set()
        context = zmq.Context.instance()
        context.setsockopt(zmq.LINGER, 0)
        context.term()
        self.nodes = []
        self.isStopped.set()
        self.isStopping.clear()
        return "OK"

    def status(self):

        type = "Input Worker" if self.isInput else "Worker"
        res = {"type": type, "errors": []}
        if self.isRestarting.is_set():
            return json.dumps({"type": type, "status": "RESTARTING"})
        if self.isStopping.is_set():
            return json.dumps({"type": type, "status": "STOPPING"})
        if self.isStopped.is_set():
            return json.dumps({"type": type, "status": "STOPPED"})
        expectedLen = len(self.graph) + 1
        if expectedLen != len(self.nodes):
            res["errors"].append(
                {
                    "error": "Worker error",
                    "message": "Graph with less nodes than expected",
                    "detail": "Try restarting the system",
                }
            )
        for node in self.nodes:
            res[node.name] = node.status()

        if res["errors"] == []:
            return json.dumps({"type": type, "status": "RUNNING"})
        res["status"] = "ERROR"
        return json.dumps(res)

    def restart(self):
        if self.isRestarting.is_set():
            return
        self.isRestarting.set()
        self.stop()
        self.update_graph()
        self.isRestarting.clear()

    def run(self):
        while True:
            res = self.subscriber.recv_string().split(";", maxsplit=2)
            try:
                match res[0]:
                    case "START":
                        res = self.update_graph()
                    case "STOP":
                        threading.Thread(target=self.stop).start()
                        res = "OK"
                    case "RESTART":
                        threading.Thread(target=self.restart).start()
                        res = "OK"
                    case "STATUS":
                        res = self.status()
                    case _:
                        raise Exception("Command {} not supported".format(res[0]))
                self.response.send_string(res)
            except Exception as e:
                self.response.send_string(
                    json.dumps(
                        [
                            {
                                "error": "Worker error",
                                "message": str(e),
                                "detail": "Try restarting the system",
                            }
                        ]
                    )
                )
                continue
