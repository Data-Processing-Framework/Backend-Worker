import zmq

ctx = zmq.Context.instance()

subscriber = ctx.socket(zmq.SUB)
subscriber.connect("tcp://127.0.0.1:5555")
subscriber.subscribe("")
while True:
    res = subscriber.recv_string()
    print(res)
