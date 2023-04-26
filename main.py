from src.controller import controller
from dotenv import load_dotenv

load_dotenv()

c = controller()
c.run()
