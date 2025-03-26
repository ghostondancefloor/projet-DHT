from StorageNode import StorageNode 

class AdvancedNode(StorageNode):
    def __init__(self, env, node_id):
        super().__init__(env, node_id)
        self.long_links = {}

    def __str__(self):
        return f"AdvancedNode({self.node_id})"

    def route_message(self, target_id, message):
        if self.node_id == target_id:
            print(f"{self.env.now:.1f}: {self} received message: {message}")
            return
        best = self.find_best_route(target_id)
        print(f"{self.env.now:.1f}: {self} routing to {target_id} via {best}")
        self.send_message(best, 'ROUTE', {'target_id': target_id, 'message': message})

    def find_best_route(self, target_id):
        if target_id in self.long_links:
            return self.long_links[target_id]
        right = self.right_neighbor
        left = self.left_neighbor
        d_right = (target_id - right.node_id) % 100
        d_left = (target_id - left.node_id) % 100
        return right if d_right < d_left else left

    def run(self):
        print(f"{self.env.now:.1f}: {self} started")
        self.env.process(self._create_long_links())
        while True:
            msg = yield self.messages.get()
            t = msg['type']; c = msg['content']; s = msg['sender']
            if t in ['JOIN_REQUEST', 'UPDATE_LEFT', 'UPDATE_RIGHT', 'STORE', 'STORE_CONFIRM', 'REPLICATE']:
                yield self.env.process(StorageNode.run(self))
            elif t == 'ROUTE':
                tid = c['target_id']; m = c['message']
                if self.node_id == tid:
                    print(f"{self.env.now:.1f}: {self} got routed message: {m}")
                else:
                    next_hop = self.find_best_route(tid)
                    self.send_message(next_hop, 'ROUTE', {'target_id': tid, 'message': m})

    def _create_long_links(self):
        yield self.env.timeout(10)
        self.long_links[self.right_neighbor.node_id] = self.right_neighbor
        print(f"{self.env.now:.1f}: {self} created long link to {self.right_neighbor}")