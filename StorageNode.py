from Node import Node 

class StorageNode(Node):
    """Extended node with data storage capabilities"""
    
    def __init__(self, env, node_id):
        super().__init__(env, node_id)
        self.data_store = {}  
        self.replicated_data = {}  
    
    def __str__(self):
        return f"StorageNode({self.node_id})"
    
    def store_data(self, key, value):
        """Store data locally"""
        self.data_store[key] = value
        print(f"{self.env.now:.1f}: {self} stored data {key}:{value}")
    
    def get_data(self, key):
        """Retrieve data by key"""
        if key in self.data_store:
            return self.data_store[key]
        return None
    
    def run(self):
        """Main node process with storage capabilities"""
        print(f"{self.env.now:.1f}: {self} started (with storage)")
        while True:
            message = yield self.messages.get()
            
            msg_type = message['type']
            sender = message['sender']
            content = message['content']
            
            # Handle base messages
            if msg_type in ['JOIN_REQUEST', 'UPDATE_LEFT', 'UPDATE_RIGHT']:
                yield self.env.process(self._handle_base_message(message))
            
            # Handle storage-specific messages
            elif msg_type == 'PUT_REQUEST':
                key = content['key']
                value = content['value']
                self.store_data(key, value)
                self.send_message(sender, 'PUT_CONFIRM', {'key': key})
            
            elif msg_type == 'GET_REQUEST':
                key = content['key']
                value = self.get_data(key)
                print(f"{self.env.now:.1f}: {self} provides data {key}:{value}")
                self.send_message(sender, 'GET_RESPONSE', {'key': key, 'value': value})
            
            elif msg_type == 'PUT_CONFIRM':
                print(f"{self.env.now:.1f}: {self} received PUT confirmation for {content['key']}")
            
            elif msg_type == 'GET_RESPONSE':
                key = content['key']
                value = content['value']
                print(f"{self.env.now:.1f}: {self} received GET response for {key}: {value}")
    
    def _handle_base_message(self, message):
        """Handle messages inherited from the base Node class"""
        msg_type = message['type']
        sender = message['sender']
        content = message['content']
        
        if msg_type == 'JOIN_REQUEST':
            self._handle_join_request(sender)
        elif msg_type == 'UPDATE_LEFT':
            self.left_neighbor = content
            print(f"{self.env.now:.1f}: {self} updated left neighbor: {self.left_neighbor}")
        elif msg_type == 'UPDATE_RIGHT':
            self.right_neighbor = content
            print(f"{self.env.now:.1f}: {self} updated right neighbor: {self.right_neighbor}")
        
        yield self.env.timeout(0)