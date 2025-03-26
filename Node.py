
import simpy

class Node:
    """Base node class for a simple ring network"""
    
    def __init__(self, env, node_id):
        self.env = env
        self.node_id = node_id
        self.left_neighbor = self
        self.right_neighbor = self
        self.messages = simpy.Store(env)
    
    def __str__(self):
        return f"Node({self.node_id})"
    
    def send_message(self, target_node, message_type, content=None):
        """Send a message to another node"""
        message = {
            'type': message_type,
            'sender': self,
            'content': content
        }
        target_node.messages.put(message)
    
    def join(self, bootstrap_node):
        """Process to join the ring via an existing node"""
        print(f"{self.env.now:.1f}: {self} requests to join the ring via {bootstrap_node}")
        self.send_message(bootstrap_node, 'JOIN_REQUEST')
        
        # Wait for the response to know its position
        response = yield self.messages.get()
        if response['type'] == 'JOIN_REPLY':
            self.left_neighbor = response['content']['left_neighbor']
            self.right_neighbor = response['content']['right_neighbor']
            
            # Notify neighbors
            self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self)
            self.send_message(self.right_neighbor, 'UPDATE_LEFT', self)
            
            print(f"{self.env.now:.1f}: {self} joined the ring between {self.left_neighbor} and {self.right_neighbor}")
    
    def leave(self):
        """Process to properly leave the ring"""
        print(f"{self.env.now:.1f}: {self} is leaving the ring")
        
        # Inform neighbors
        self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self.right_neighbor)
        self.send_message(self.right_neighbor, 'UPDATE_LEFT', self.left_neighbor)
        
        print(f"{self.env.now:.1f}: {self} has left the ring")
    
    def run(self):
        """Main node process to handle messages"""
        print(f"{self.env.now:.1f}: {self} started")
        while True:
            message = yield self.messages.get()
            
            msg_type = message['type']
            sender = message['sender']
            content = message['content']
            
            # Handle different message types
            if msg_type == 'JOIN_REQUEST':
                self._handle_join_request(sender)
            elif msg_type == 'UPDATE_LEFT':
                self.left_neighbor = content
                print(f"{self.env.now:.1f}: {self} updated left neighbor: {self.left_neighbor}")
            elif msg_type == 'UPDATE_RIGHT':
                self.right_neighbor = content
                print(f"{self.env.now:.1f}: {self} updated right neighbor: {self.right_neighbor}")
            elif msg_type == 'MSG':
                print(f"{self.env.now:.1f}: {self} received message: {content}")
    
    def _handle_join_request(self, sender):
        """Handle a join request from another node"""
        # Find the correct position in the ring
        current = self
        next_node = self.right_neighbor
        
        # If we are the only node in the ring
        if current == next_node:
            self.right_neighbor = sender
            self.left_neighbor = sender
            self.send_message(sender, 'JOIN_REPLY', {
                'left_neighbor': self, 
                'right_neighbor': self
            })
            return
        
        # Traverse the ring to find the position
        while True:
            # Special case: ID at beginning/end of the ring
            if (current.node_id > next_node.node_id and 
                (sender.node_id > current.node_id or sender.node_id < next_node.node_id)):
                break
                
            # Standard case: ID between two consecutive nodes
            if current.node_id < sender.node_id < next_node.node_id:
                break
            
            # Move to the next node
            current = next_node
            next_node = current.right_neighbor
            
            # If we've made a full circle around the ring
            if current == self:
                break
        
        # Send the response with the new neighbors
        self.send_message(sender, 'JOIN_REPLY', {
            'left_neighbor': current, 
            'right_neighbor': next_node
        })