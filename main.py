from src.controller import controller
from dotenv import load_dotenv
import os

load_dotenv()

if os.getenv("DEBUG") == "1":
    import time

    while True:
        time.sleep(10)

else:
    c = controller()
    c.run()
