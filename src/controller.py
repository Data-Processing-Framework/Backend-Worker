import zmq
import os
import json
from importlib import import_module
from src.modules.transform import transform
from src.modules.input import input
from src.broker import broker


class controller:
    def __init__(self) -> None:
        context = zmq.Context()
        self.subscriber = context.socket(zmq.SUB)
        self.subscriber.connect(os.getenv("CONTROLLER_REQUEST_ADDRESS"))
        self.subscriber.subscribe("")
        self.response = context.socket(zmq.PUB)
        self.response.connect(os.getenv("CONTROLLER_RESPONSE_ADDRESS"))
        self.isInput = int(os.getenv("IS_INPUT"))

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
                return input(node["name"], module.process_item, 1)
            case "Transform":
                # TODO Decidir si volem que el client programi el process_item o tot el modul per tenir mes llibertat
                return transform(node["name"], node["inputs"], module.process_item, 1)
            case "Output":
                pass
            case _:
                pass
        return "OK"

    def update_graph(self):
        graph_file = open("./src/data/graph.json", "r")
        graph = json.load(graph_file)
        if self.isInput:
            b = broker()
            self.nodes.append(b)
            b.start()
            self.graph = [g for g in graph if g["type"] == "Input"]
        else:
            self.graph = [g for g in graph if g["type"] != "Input"]

        for a in self.graph:
            node = self.create_node(a)
            if node != "OK":
                self.nodes.append(node)
                node.start()

        return "OK"

    def stop(self):
        context = zmq.Context.instance()
        context.term()
        for node in self.nodes:
            if isinstance(node, broker):
                node.stop()
        self.nodes = []
        return "OK"

    def status(self):
        res = {"errors": []}
        expectedLen = len(self.graph)
        if self.isInput:
            expectedLen += 1
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
