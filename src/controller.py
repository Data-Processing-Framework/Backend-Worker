import zmq
from dotenv import load_dotenv
import os
import json


class controller:
    def __init__(self) -> None:
        ctx = zmq.Context.instance()

        self.subscriber = ctx.socket(zmq.SUB)
        self.subscriber.connect(os.getenv("controller_request_bus_address"))
        self.subscriber.subscribe("")
        res = self.update_graph('{"hola":3}')
        if res != "OK":
            raise Exception(res)

    def update_graph(self, graph=None):
        try:
            graph_file = open("./src/data/graph.json", "w+")
            if not graph:
                self.graph = json.dump(graph_file)
            else:
                self.graph = json.loads(graph)
                json.dump(graph, graph_file)

            for a in self.graph:
                self.nodes = a
        except Exception as e:
            return str(e)

    def start(self):
        self.update_graph()

    def stop(self):
        for a in self.nodes:
            pass

    def create_module(self):
        pass

    def run(self):
        while True:
            # CREATE;MODULUE;{"name":"creator", code:"def..."}
            res = self.subscriber.recv_string().split(";", maxsplit=2)
            match res[0]:
                case "START":
                    pass
                case "CREATE":
                    match res[1]:
                        case "MODULE":
                            self.create_module(res[2])
                        case "GRAPH":
                            self.update_graph(res[2])


controller()
