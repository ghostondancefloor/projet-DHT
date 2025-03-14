import simpy 
import random 
import hashlib


class DHTNode:
    def __init__(self, env, node_id):
        self.env = env
        self.node_id = node_id
        self.data = {}
        self.routing_table = {}
        self.action = env.process(self.run())
    
    def run(self):
        while True:
            yield self.env.timeout(random.randint(1, 5))
            print(f"[{self.env.now}] Node {self.node_id} is active")
    
    def store(self, key, value):
        hashed_key = hashlib.sha1(key.encode()).hexdigest()[:5] 
        self.data[hashed_key] = value
        print(f"Node {self.node_id} stored {key} -> {hashed_key}")
        
    def lookup(self, key):
        hashed_key = hashlib.sha1(key.encode()).hexdigest()[:5]
        return self.data.get(hashed_key, "Not Found")
    
    
env = simpy.Environment()

nodes = [DHTNode(env, i) for i in range(5)]

nodes[0].store("hello", "world")

print("Lookup result:", nodes[0].lookup("hello"))

env.run(until=10)