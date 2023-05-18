import zmq
import os
import json
from importlib import import_module
from src.modules.transform import transform
from src.modules.input import input
from src.modules.output import output
from src.broker import broker
from src.internal_bus import internal_bus
import threading
from src.logger import logger
import mysql.connector
import logging
import time
import socket


class controller:
    def __init__(self) -> None:
        context = zmq.Context()
        self.graph = []
        self.subscriber = context.socket(zmq.SUB)
        self.subscriber.connect(os.getenv("CONTROLLER_REQUEST_ADDRESS"))
        self.subscriber.subscribe("")
        self.status_bus = context.socket(zmq.PUB)
        self.status_bus.connect(os.getenv("CONTROLLER_STATUS_ADDRESS"))
        self.isInput = int(os.getenv("IS_INPUT"))
        self.isRestarting = threading.Event()
        self.isStopped = threading.Event()
        self.isStopped.set()
        self.isStopping = threading.Event()
        self.init_logger()

        self.nodes = []
        self.n_nodes = 0
        self.update_graph()

    def init_logger(self):
        db_server = os.getenv("LOGGING_DB_ADDRESS")
        db_user = os.getenv("LOGGING_DB_USER")
        db_password = os.getenv("LOGGING_DB_PASSWORD")
        db_dbname = os.getenv("LOGGING_DB_NAME")
        log_conn = ""
        while not isinstance(
            log_conn, mysql.connector.connection_cext.CMySQLConnection
        ):
            try:
                log_conn = mysql.connector.connect(
                    host=db_server,
                    user=db_user,
                    password=db_password,
                    database=db_dbname,
                )
            except Exception:
                print("Waiting for logging database")
                time.sleep(1)
                continue
        log_cursor = log_conn.cursor()
        logdb = logger(log_conn, log_cursor)
        l = logging.getLogger("")
        l.addHandler(logdb)
        l.setLevel(logging.INFO)

    def create_node(self, node):
        try:
            module_path = "./src/data/modules/{}.py".format(node["module"])
            if not os.path.isfile(module_path):
                raise Exception("Module {} not found".format(node["module"]))
            module = import_module("src.data.modules." + node["module"])

            match (node["type"]):
                case "Input":
                    return input(node["name"], module.process_item, 1)
                case "Transform":
                    return transform(
                        node["name"], node["inputs"], module.process_item, 1
                    )
                case "Output":
                    return output(node["name"], node["inputs"], module.process_item, 1)
                case _:
                    return "Error"
        except Exception as e:
            print(e)
            return "Error"

    def update_graph(self):
        if not self.isStopped.is_set() or self.isStopping.is_set():
            return "System is running"
        self.isStopped.clear()
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
            if node != "Error":
                self.nodes.append(node)
                node.start()
        self.status()

    def stop(self):
        if self.isStopped.is_set() or self.isStopping.is_set():
            return
        self.isStopping.set()
        self.status()
        context = zmq.Context.instance()
        context.setsockopt(zmq.LINGER, 0)
        context.term()
        self.nodes = []
        self.isStopped.set()
        self.isStopping.clear()
        self.status()

    def status(self):
        type = "Input Worker" if self.isInput else "Worker"
        res = {
            "type": type,
            "errors": [],
            "services": {},
            "id": socket.gethostname(),
            "status": "RUNNING",
        }

        if self.isRestarting.is_set():
            res["status"] = "RESTARTING"
        elif self.isStopping.is_set():
            res["status"] = "STOPPING"
        elif self.isStopped.is_set():
            res["status"] = "STOPPED"
        else:
            expectedLen = len(self.graph) + 1
            if expectedLen != len(self.nodes):
                res["errors"].append(
                    {
                        "error": "Worker error",
                        "message": "Graph with less nodes than expected",
                        "detail": "Try restarting the system",
                    }
                )
                res["status"] = "ERROR"
            for node in self.nodes:
                status = node.status()
                res["services"][node.name] = status
                if status["errors"] != []:
                    res["status"] = "ERROR"

        self.status_bus.send_string(json.dumps(res))

    def restart(self):
        if self.isRestarting.is_set():
            return
        self.isRestarting.set()
        self.stop()
        self.update_graph()
        self.isRestarting.clear()
        self.status()

    def run(self):
        while True:
            command = self.subscriber.recv_string()
            try:
                match command:
                    case "START":
                        self.update_graph()
                    case "STOP":
                        threading.Thread(target=self.stop).start()
                    case "RESTART":
                        threading.Thread(target=self.restart).start()
                    case "STATUS":
                        self.status()
                    case _:
                        raise Exception("Command {} not supported".format(command))
            except Exception as e:
                print(e)
                continue
