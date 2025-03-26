from StorageNode import StorageNode 

class AdvancedNode(StorageNode):
    """Extended node with advanced routing capabilities"""
    
    def __init__(self, env, node_id):
        super().__init__(env, node_id)
        self.long_links = {}  # Long distance links for faster routing
    
    def __str__(self):
        return f"AdvancedNode({self.node_id})"
    
    def add_long_link(self, target_node):
        """Add a long-distance link to another node"""
        if target_node != self:
            self.long_links[target_node.node_id] = target_node
            print(f"{self.env.now:.1f}: {self} established long link to {target_node}")
    
    def find_best_route(self, target_id):
        """Find the best next hop to reach a target ID"""
        # If we have a direct long link
        if target_id in self.long_links:
            return self.long_links[target_id]
        
        # Otherwise, use standard ring routing
        left_distance = self._circular_distance(self.left_neighbor.node_id, target_id)
        right_distance = self._circular_distance(self.right_neighbor.node_id, target_id)
        
        if left_distance < right_distance:
            return self.left_neighbor
        else:
            return self.right_neighbor
    
    def _circular_distance(self, from_id, to_id):
        """Calculate the circular distance between two IDs in the ring (assuming max 100)"""
        forward_distance = (to_id - from_id) % 100
        backward_distance = (from_id - to_id) % 100
        return min(forward_distance, backward_distance)
    
    def run(self):
        """Main node process with advanced routing"""
        print(f"{self.env.now:.1f}: {self} started (with advanced routing)")
        while True:
            message = yield self.messages.get()
            
            msg_type = message['type']
            sender = message['sender']
            content = message['content']
            
            # Handle storage node messages
            if msg_type in ['JOIN_REQUEST', 'UPDATE_LEFT', 'UPDATE_RIGHT', 
                           'PUT_REQUEST', 'GET_REQUEST', 'PUT_CONFIRM', 'GET_RESPONSE']:
                yield self.env.process(super().run())
            
            # Handle advanced routing messages
            elif msg_type == 'ROUTE_REQUEST':
                target_id = content['target_id']
                route_data = content['data']
                hops = content.get('hops', 0)
                
                # If this is the target
                if target_id == self.node_id:
                    print(f"{self.env.now:.1f}: {self} received routed data: {route_data} after {hops} hops")
                else:
                    # Find best next hop
                    next_hop = self.find_best_route(target_id)
                    content['hops'] = hops + 1
                    print(f"{self.env.now:.1f}: {self} routing message to {next_hop} (target: {target_id})")
                    self.send_message(next_hop, 'ROUTE_REQUEST', content)
            
            elif msg_type == 'LONG_LINK_REQUEST':
                self.add_long_link(sender)
                self.send_message(sender, 'LONG_LINK_CONFIRM', {'node_id': self.node_id})
            
            elif msg_type == 'LONG_LINK_CONFIRM':
                target_id = content['node_id']
                self.add_long_link(sender)