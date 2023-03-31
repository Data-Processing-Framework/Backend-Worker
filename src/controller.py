import zmq
from dotenv import load_dotenv
import os
import json


class controller:
    def __init__(self) -> None:
        pass
        ctx = zmq.Context.instance()

        self.subscriber = ctx.socket(zmq.SUB)
        self.subscriber.connect(os.getenv("controller_request_bus_address"))
        self.subscriber.subscribe("")

    def update_graph(self, graph=None):
        try:
            graph_file = open("./data/graph.json", "w")
            if not graph:
                self.graph = json.loads(graph_file)
            else:
                self.graph = json.loads(graph)
                json.dump(graph, graph_file)
        except Exception as e:
            return str(e)

        for a in self.graph:
            pass

    def start(self):
        pass

    def run(self):
        while True:
            res = self.subscriber.recv_string().split(";", maxsplit=2)
            match res[0]:
                case "START":
                    pass
