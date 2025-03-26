from Node import Node 
import hashlib

class StorageNode(Node):
    def __init__(self, env, node_id):
        super().__init__(env, node_id)
        self.data = {}
        self.replicas = {}

    def __str__(self):
        return f"StorageNode({self.node_id})"

    def compute_key_location(self, key):
        h = hashlib.sha1(str(key).encode()).hexdigest()
        target_id = int(h, 16) % 100
        current = self
        while True:
            right = current.right_neighbor
            if ((current.node_id < target_id <= right.node_id) or
                (current.node_id > right.node_id and
                 (target_id > current.node_id or target_id <= right.node_id))):
                return right
            current = right
            if current == self:
                return self

    def store(self, key, value):
        target = self.compute_key_location(key)
        if target == self:
            print(f"{self.env.now:.1f}: {self} storing {key}={value} (primary)")
            self.data[key] = value
            self.send_message(self.left_neighbor, 'REPLICATE', {'key': key, 'value': value})
            if self.right_neighbor != self.left_neighbor:
                self.send_message(self.right_neighbor, 'REPLICATE', {'key': key, 'value': value})
        else:
            self.send_message(target, 'STORE', {'key': key, 'value': value, 'origin': self})

    def run(self):
        print(f"{self.env.now:.1f}: {self} started")
        while True:
            message = yield self.messages.get()
            t = message['type']
            s = message['sender']
            c = message['content']

            if t == 'JOIN_REQUEST':
                self._handle_join_request(s)
            elif t == 'UPDATE_LEFT':
                self.left_neighbor = c
            elif t == 'UPDATE_RIGHT':
                self.right_neighbor = c
            elif t == 'STORE':
                k = c['key']; v = c['value']; o = c['origin']
                self.data[k] = v
                self.send_message(self.left_neighbor, 'REPLICATE', {'key': k, 'value': v})
                if self.right_neighbor != self.left_neighbor:
                    self.send_message(self.right_neighbor, 'REPLICATE', {'key': k, 'value': v})
                if o != self:
                    self.send_message(o, 'STORE_CONFIRM', {'key': k})
            elif t == 'STORE_CONFIRM':
                print(f"{self.env.now:.1f}: {self} confirmed storage of {c['key']}")
            elif t == 'REPLICATE':
                self.replicas[c['key']] = c['value']