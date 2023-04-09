from src.controller import controller
from threading import Thread
from src import broker

Thread(target=broker.run).start()
c = controller()
c.run()
