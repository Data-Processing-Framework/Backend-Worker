import zmq
from dotenv import load_dotenv
import os
import json
from importlib import import_module
from src.modules.transform import transform
from threading import Thread
import time

load_dotenv()


class controller:
    def __init__(self) -> None:
        context = zmq.Context()
        self.subscriber = context.socket(zmq.SUB)
        self.subscriber.connect(os.getenv("controller_request_bus_address"))
        self.subscriber.subscribe("")
        self.response = context.socket(zmq.PUB)
        self.subscriber.connect(os.getenv("controller_response_bus_address"))

        self.nodes = []
        self.n_nodes = 0
        res = self.update_graph()
        if res != "OK":
            raise Exception(res)

    def create_node(self, node):
        module_path = "./src/data/modules/{}.py".format(node["module"])
        if not os.path.isfile(module_path):
            raise Exception("Module {} not found".format(node["module"]))
        module = import_module("src.data.modules." + node["module"])
        match (node["type"]):
            case "Input":
                pass
            case "Transform":
                # TODO Decidir si volem que el client programi el process_item o tot el modul per tenir mes llibertat
                return transform(node["name"], node["inputs"], module.process_item, 10)
            case "Output":
                pass
            case _:
                pass
        return "OK"

    def update_graph(self):
        graph_file = open("./src/data/graph.json", "r")
        self.graph = json.load(graph_file)

        for a in self.graph:
            node = self.create_node(a)
            self.nodes.append(node)
            node.start()

        return "OK"

    def stop(self):
        context = zmq.Context.instance()
        context.term()
        self.nodes = []
        return "OK"

    def status(self):
        res = {"errors": []}
        if len(self.graph) != len(self.nodes):
            res["errors"].append(
                {
                    "error": "Worker error",
                    "message": "Graph with less nodes than expected",
                    "detail": "Try restarting the system",
                }
            )
        for node in self.nodes:
            res[node.name] = node.status()

        return json.dumps(res)

    def run(self):
        while True:
            res = self.subscriber.recv_string().split(";", maxsplit=2)
            try:
                match res[0]:
                    case "START":
                        res = self.update_graph()
                    case "STOP":
                        res = self.stop()
                    case "RESTART":
                        self.stop()
                        res = self.update_graph()
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
