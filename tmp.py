import zmq

context = zmq.Context()
subscriber = context.socket(zmq.SUB)
subscriber.connect("tcp://127.0.0.1:5557")

while True:
    message = subscriber.recv()
    print(message)
