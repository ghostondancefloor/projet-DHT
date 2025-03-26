import hashlib
import random


class DHTNode:
    """Represents a node in the DHT network."""

    def __init__(self, env, node_id):
        self.env = env
        self.node_id = node_id
        self.routing_table = {}  
        self.data_store = {} 
        self.action = env.process(self.run())  

    def run(self):
        """Simulated process for node activity."""
        while True:
            yield self.env.timeout(random.randint(1, 5)) 
            print(f"[{self.env.now}] Node {self.node_id} is running.")

    def send(self, recipient_id, message, dht):
        """Send a message to a node in the DHT."""
        recipient = dht.get_node(recipient_id)
        if recipient:
            recipient.deliver(self.node_id, message)
        else:
            print(f"[{self.env.now}] Node {self.node_id} -> Node {recipient_id} not found!")

    def deliver(self, sender_id, message):
        """Deliver a message to this node."""
        print(f"[{self.env.now}] Node {self.node_id} received message from Node {sender_id}: {message.content} (ID: {message.msg_id})")

    def store(self, key, value):
        """Stores a value in the node using a hashed key."""
        hashed_key = hashlib.sha1(key.encode()).hexdigest()[:8]
        self.data_store[hashed_key] = value
        print(f"[{self.env.now}] Node {self.node_id} stored {key} -> {hashed_key}")

    def lookup(self, key):
        """Retrieves a value using the hashed key."""
        hashed_key = hashlib.sha1(key.encode()).hexdigest()[:8]
        return self.data_store.get(hashed_key, "Not Found")

