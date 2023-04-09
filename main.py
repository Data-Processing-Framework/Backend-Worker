from src.controller import controller
from threading import Thread
from dotenv import load_dotenv
from src import broker

load_dotenv()


Thread(target=broker.run).start()
c = controller()
c.run()
