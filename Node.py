
import simpy

class Node:
    def __init__(self, env, node_id):
        self.env = env
        self.node_id = node_id
        self.left_neighbor = self
        self.right_neighbor = self
        self.messages = simpy.Store(env)

    def __str__(self):
        return f"Node({self.node_id})"

    def send_message(self, target, message_type, content=None):
        message = {
            'type': message_type,
            'sender': self,
            'content': content
        }
        target.messages.put(message)

    def join(self, bootstrap_node):
        print(f"{self.env.now:.1f}: {self} requests to join via {bootstrap_node}")
        self.send_message(bootstrap_node, 'JOIN_REQUEST')
        response = yield self.messages.get()
        if response['type'] == 'JOIN_RESPONSE':
            self.left_neighbor = response['content']['left']
            self.right_neighbor = response['content']['right']
            self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self)
            self.send_message(self.right_neighbor, 'UPDATE_LEFT', self)
            print(f"{self.env.now:.1f}: {self} joined between {self.left_neighbor} and {self.right_neighbor}")

    def leave(self):
        print(f"{self.env.now:.1f}: {self} is leaving the ring")
        self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self.right_neighbor)
        self.send_message(self.right_neighbor, 'UPDATE_LEFT', self.left_neighbor)

    def run(self):
        print(f"{self.env.now:.1f}: {self} started")
        while True:
            message = yield self.messages.get()
            msg_type = message['type']
            sender = message['sender']
            content = message['content']

            if msg_type == 'JOIN_REQUEST':
                self._handle_join_request(sender)
            elif msg_type == 'UPDATE_LEFT':
                self.left_neighbor = content
                print(f"{self.env.now:.1f}: {self} updated left neighbor to {self.left_neighbor}")
            elif msg_type == 'UPDATE_RIGHT':
                self.right_neighbor = content
                print(f"{self.env.now:.1f}: {self} updated right neighbor to {self.right_neighbor}")
            elif msg_type == 'PING':
                print(f"{self.env.now:.1f}: {self} received ping from {sender}")
                self.send_message(sender, 'PONG')
            elif msg_type == 'PONG':
                print(f"{self.env.now:.1f}: {self} received pong from {sender}")

    def _handle_join_request(self, new_node):
        current = self
        next_node = self.right_neighbor
        while True:
            if ((current.node_id < new_node.node_id <= next_node.node_id) or
                (current.node_id > next_node.node_id and
                 (new_node.node_id > current.node_id or new_node.node_id <= next_node.node_id))):
                break
            current = next_node
            next_node = current.right_neighbor
            if current == self:
                break
        self.send_message(new_node, 'JOIN_RESPONSE', {'left': current, 'right': next_node})