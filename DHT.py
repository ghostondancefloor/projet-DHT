import simpy
from Data import Data
from DHTNode import DHTNode


class DHT:
    """Manages the Distributed Hash Table, allowing nodes to join and leave."""

    def __init__(self, env):
        self.env = env
        self.nodes = {}  

    def join(self, node):
        """Add a new node to the DHT."""
        self.nodes[node.node_id] = node
        for other_node in self.nodes.values():
            if other_node.node_id != node.node_id:
                other_node.routing_table[node.node_id] = node
                node.routing_table[other_node.node_id] = other_node
        print(f"[{self.env.now}] Node {node.node_id} joined the DHT.")

    def leave(self, node_id):
        """Remove a node from the DHT."""
        if node_id in self.nodes:
            del self.nodes[node_id]
            for node in self.nodes.values():
                node.routing_table.pop(node_id, None)
            print(f"[{self.env.now}] Node {node_id} left the DHT.")
        else:
            print(f"[{self.env.now}] Node {node_id} is not in the DHT.")

    def get_node(self, node_id):
        """Retrieve a node from the DHT."""
        return self.nodes.get(node_id, None)


env = simpy.Environment()
dht = DHT(env)

nodes = [DHTNode(env, i) for i in range(5)]
for node in nodes:
    dht.join(node)

nodes[0].store("hello", "world")

msg = Data("Hello, Node 2!")
nodes[1].send(2, msg, dht)

print(f"Lookup result in Node 0: {nodes[0].lookup('hello')}")

dht.leave(3)

env.run(until=10)