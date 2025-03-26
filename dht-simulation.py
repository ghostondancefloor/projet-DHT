#!/usr/bin/env python3
"""
Simplified DHT Simulator
========================
A reliable, simplified implementation of a DHT (Distributed Hash Table) simulator.
"""

import simpy
import random
import hashlib
import argparse
import sys
from enum import Enum

class DemoLevel(Enum):
    BASIC = "basic"       # Just a ring with node join/leave
    STORAGE = "storage"   # Adding storage capabilities
    ADVANCED = "advanced" # Adding advanced routing

class Node:
    """Basic node in the DHT ring"""
    
    def __init__(self, env, node_id):
        self.env = env
        self.node_id = node_id
        self.left_neighbor = self
        self.right_neighbor = self
        self.messages = simpy.Store(env)
    
    def __str__(self):
        return f"Node({self.node_id})"
    
    def __repr__(self):
        return self.__str__()
    
    def send_message(self, target, message_type, content=None):
        """Send a message to another node"""
        message = {
            'type': message_type,
            'sender': self,
            'content': content
        }
        target.messages.put(message)
    
    def join(self, bootstrap_node):
        """Join the DHT ring via a bootstrap node"""
        print(f"{self.env.now:.1f}: {self} requests to join via {bootstrap_node}")
        self.send_message(bootstrap_node, 'JOIN_REQUEST')
        
        # Wait for the join response
        response = yield self.messages.get()
        if response['type'] == 'JOIN_RESPONSE':
            self.left_neighbor = response['content']['left']
            self.right_neighbor = response['content']['right']
            
            # Update our neighbors
            self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self)
            self.send_message(self.right_neighbor, 'UPDATE_LEFT', self)
            
            print(f"{self.env.now:.1f}: {self} joined between {self.left_neighbor} and {self.right_neighbor}")
    
    def leave(self):
        """Leave the DHT ring"""
        print(f"{self.env.now:.1f}: {self} is leaving the ring")
        
        # Update neighbors to connect to each other
        self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self.right_neighbor)
        self.send_message(self.right_neighbor, 'UPDATE_LEFT', self.left_neighbor)
    
    def run(self):
        """Main node process"""
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
        """Handle a request from a new node to join the ring"""
        # If we're alone in the ring
        if self.right_neighbor == self:
            print(f"{self.env.now:.1f}: {self} accepts {new_node} (first join)")
            self.send_message(new_node, 'JOIN_RESPONSE', {'left': self, 'right': self})
            return
        
        # Find the correct position based on node_id
        current = self
        next_node = self.right_neighbor
        
        while True:
            # Check if this is the correct position (between current and next_node)
            # Handle wrapping around the ring
            if ((current.node_id < new_node.node_id < next_node.node_id) or
                (current.node_id > next_node.node_id and 
                  (new_node.node_id > current.node_id or new_node.node_id < next_node.node_id))):
                break
            
            current = next_node
            next_node = current.right_neighbor
            
            # If we've gone all the way around
            if current == self:
                break
        
        print(f"{self.env.now:.1f}: {self} inserting {new_node} between {current} and {next_node}")
        self.send_message(new_node, 'JOIN_RESPONSE', {'left': current, 'right': next_node})

class StorageNode(Node):
    """Node with data storage capabilities"""
    
    def __init__(self, env, node_id):
        super().__init__(env, node_id)
        self.data = {}  # Storage for (key, value) pairs
        self.replicas = {}  # Storage for replicated data
    
    def __str__(self):
        return f"StorageNode({self.node_id})"
    
    def compute_key_location(self, key):
        """Compute which node is responsible for a key"""
        # Hash the key to get a node_id in our range
        hash_obj = hashlib.sha1(str(key).encode())
        target_id = int(hash_obj.hexdigest(), 16) % 100  # Assume node_ids are 0-99
        
        # Find the node responsible for this key (node with smallest id >= target_id)
        # Start from ourselves and go clockwise
        current = self
        visited = set([self.node_id])
        
        while True:
            # If current node is responsible for this key
            if current.node_id >= target_id or current.right_neighbor.node_id < target_id:
                return current
            
            # Move to the next node
            current = current.right_neighbor
            
            # Prevent infinite loop if ring is broken
            if current.node_id in visited:
                # If we've gone all the way around, return self
                return self
            
            visited.add(current.node_id)
    
    def store(self, key, value):
        """Store a (key, value) pair"""
        responsible_node = self.compute_key_location(key)
        
        if responsible_node == self:
            # We are responsible for this key
            print(f"{self.env.now:.1f}: {self} storing {key}={value} (primary)")
            self.data[key] = value
            
            # Replicate to neighbors
            self.send_message(self.left_neighbor, 'REPLICATE', {'key': key, 'value': value})
            if self.right_neighbor != self.left_neighbor:  # Avoid duplicate if only 2 nodes
                self.send_message(self.right_neighbor, 'REPLICATE', {'key': key, 'value': value})
        else:
            # Forward to the responsible node
            print(f"{self.env.now:.1f}: {self} forwarding storage of {key} to {responsible_node}")
            self.send_message(responsible_node, 'STORE', {'key': key, 'value': value, 'origin': self})
    
    def retrieve(self, key):
        """Retrieve a value by key"""
        responsible_node = self.compute_key_location(key)
        
        if responsible_node == self:
            # We are responsible for this key
            if key in self.data:
                value = self.data[key]
                print(f"{self.env.now:.1f}: {self} retrieving {key}={value} (primary)")
                return value
            elif key in self.replicas:
                value = self.replicas[key]
                print(f"{self.env.now:.1f}: {self} retrieving {key}={value} (replica)")
                return value
            else:
                print(f"{self.env.now:.1f}: {self} key {key} not found")
                return None
        else:
            # Forward to the responsible node
            print(f"{self.env.now:.1f}: {self} forwarding retrieval of {key} to {responsible_node}")
            self.send_message(responsible_node, 'RETRIEVE', {'key': key, 'origin': self})
            return None  # Async operation, response will come later
    
    def run(self):
        """Main node process with storage capabilities"""
        print(f"{self.env.now:.1f}: {self} started")
        while True:
            message = yield self.messages.get()
            
            msg_type = message['type']
            sender = message['sender']
            content = message['content'] if 'content' in message else None
            
            # Handle base node messages
            if msg_type == 'JOIN_REQUEST':
                self._handle_join_request(sender)
            elif msg_type == 'UPDATE_LEFT':
                self.left_neighbor = content
                print(f"{self.env.now:.1f}: {self} updated left neighbor to {self.left_neighbor}")
            elif msg_type == 'UPDATE_RIGHT':
                self.right_neighbor = content
                print(f"{self.env.now:.1f}: {self} updated right neighbor to {self.right_neighbor}")
            
            # Handle storage-specific messages
            elif msg_type == 'STORE':
                key = content['key']
                value = content['value']
                origin = content.get('origin', sender)
                
                # Store locally
                self.data[key] = value
                print(f"{self.env.now:.1f}: {self} stored {key}={value} (primary)")
                
                # Replicate to neighbors
                self.send_message(self.left_neighbor, 'REPLICATE', {'key': key, 'value': value})
                if self.right_neighbor != self.left_neighbor:
                    self.send_message(self.right_neighbor, 'REPLICATE', {'key': key, 'value': value})
                
                # Confirm to the original requester
                if origin != self:
                    self.send_message(origin, 'STORE_CONFIRM', {'key': key})
            
            elif msg_type == 'STORE_CONFIRM':
                key = content['key']
                print(f"{self.env.now:.1f}: {self} received confirmation for storing {key}")
            
            elif msg_type == 'RETRIEVE':
                key = content['key']
                origin = content.get('origin', sender)
                
                # Retrieve locally
                if key in self.data:
                    value = self.data[key]
                    print(f"{self.env.now:.1f}: {self} retrieved {key}={value} (primary)")
                elif key in self.replicas:
                    value = self.replicas[key]
                    print(f"{self.env.now:.1f}: {self} retrieved {key}={value} (replica)")
                else:
                    value = None
                    print(f"{self.env.now:.1f}: {self} key {key} not found")
                
                # Send response to the original requester
                if origin != self:
                    self.send_message(origin, 'RETRIEVE_RESPONSE', {'key': key, 'value': value})
            
            elif msg_type == 'RETRIEVE_RESPONSE':
                key = content['key']
                value = content['value']
                print(f"{self.env.now:.1f}: {self} received response for {key}={value}")
            
            elif msg_type == 'REPLICATE':
                key = content['key']
                value = content['value']
                self.replicas[key] = value
                print(f"{self.env.now:.1f}: {self} replicated {key}={value}")

class AdvancedNode(StorageNode):
    """Node with advanced routing capabilities"""
    
    def __init__(self, env, node_id):
        super().__init__(env, node_id)
        self.long_links = {}  # Additional routing links
    
    def __str__(self):
        return f"AdvancedNode({self.node_id})"
    
    def create_long_links(self):
        """Create long-distance links to optimize routing"""
        print(f"{self.env.now:.1f}: {self} creating long links")
        
        # Create links at power-of-two distances (like Chord)
        for i in range(1, 6):  # Links at distances 2^1, 2^2, 2^3, 2^4, 2^5
            target_id = (self.node_id + 2**i) % 100  # Assume node_ids are 0-99
            
            # Find the node closest to this target_id
            # For simplicity in this simulation, we'll just add the right neighbor
            # as a long link if we only have one node in the ring
            if self.right_neighbor == self:
                continue
                
            # For demo purposes, just establish a link to the right neighbor
            # In a real implementation, we would search for the appropriate node
            self.long_links[self.right_neighbor.node_id] = self.right_neighbor
            print(f"{self.env.now:.1f}: {self} added long link to {self.right_neighbor}")
            
            # Only add one link in this simplified version
            break
    
    def find_best_route(self, target_id):
        """Find the best next hop to reach a target node ID"""
        # Check if we have a direct long link
        if target_id in self.long_links:
            return self.long_links[target_id]
        
        # Find the closest preceding node in long links
        best_distance = float('inf')
        best_node = None
        
        for link_id, link_node in self.long_links.items():
            # Calculate circular distance
            clockwise_dist = (target_id - link_id) % 100
            if 0 < clockwise_dist < best_distance:
                best_distance = clockwise_dist
                best_node = link_node
        
        if best_node:
            return best_node
        
        # Fall back to regular ring routing
        clockwise_right = (target_id - self.right_neighbor.node_id) % 100
        clockwise_left = (target_id - self.left_neighbor.node_id) % 100
        
        if clockwise_right < clockwise_left:
            return self.right_neighbor
        else:
            return self.left_neighbor
    
    def route_message(self, target_id, message):
        """Route a message to a node with target_id"""
        # If we are the target
        if self.node_id == target_id:
            print(f"{self.env.now:.1f}: {self} received routed message: {message}")
            return
        
        # Find best next hop
        next_hop = self.find_best_route(target_id)
        print(f"{self.env.now:.1f}: {self} routing message to {target_id} via {next_hop}")
        
        # Forward the message
        self.send_message(next_hop, 'ROUTE', {
            'target_id': target_id,
            'message': message,
            'hops': message.get('hops', 0) + 1,
            'source': message.get('source', self)
        })
    
    def run(self):
        """Main node process with advanced routing"""
        print(f"{self.env.now:.1f}: {self} started")
        
        # After a short delay, create long links
        self.env.process(self._delayed_link_creation())
        
        while True:
            message = yield self.messages.get()
            
            msg_type = message['type']
            sender = message['sender']
            content = message['content'] if 'content' in message else None
            
            # Handle storage node messages
            if msg_type in ['JOIN_REQUEST', 'UPDATE_LEFT', 'UPDATE_RIGHT', 'STORE', 
                           'STORE_CONFIRM', 'RETRIEVE', 'RETRIEVE_RESPONSE', 'REPLICATE']:
                # Use parent class handler
                yield self.env.process(super().run())
            
            # Handle advanced routing messages
            elif msg_type == 'ROUTE':
                target_id = content['target_id']
                payload = content['message']
                hops = content.get('hops', 0)
                source = content.get('source', sender)
                
                # If we are the target
                if self.node_id == target_id:
                    print(f"{self.env.now:.1f}: {self} received routed message from {source} after {hops} hops: {payload}")
                    # Send acknowledgment back to source
                    self.send_message(source, 'ROUTE_ACK', {
                        'target_id': target_id,
                        'hops': hops,
                        'message': f"Received: {payload}"
                    })
                else:
                    # Forward using our routing strategy
                    next_hop = self.find_best_route(target_id)
                    print(f"{self.env.now:.1f}: {self} forwarding message to {target_id} via {next_hop} (hop {hops})")
                    
                    # Forward the message
                    self.send_message(next_hop, 'ROUTE', {
                        'target_id': target_id,
                        'message': payload,
                        'hops': hops + 1,
                        'source': source
                    })
            
            elif msg_type == 'ROUTE_ACK':
                print(f"{self.env.now:.1f}: {self} received acknowledgment: {content['message']} (took {content['hops']} hops)")
    
    def _delayed_link_creation(self):
        """Process to create long links after a delay"""
        # Wait for the ring to stabilize
        yield self.env.timeout(10)
        
        # Create long links
        self.create_long_links()

def run_basic_simulation(env, max_nodes=10, duration=100):
    """Run a basic DHT simulation"""
    print("Starting basic DHT simulation...")
    
    # Create first node
    first_node = Node(env, node_id=0)
    env.process(first_node.run())
    
    nodes = [first_node]
    
    # Process to add new nodes periodically
    def node_creator():
        next_id = 1
        while next_id < max_nodes:
            yield env.timeout(random.randint(3, 8))
            
            # Create a new node
            new_node = Node(env, node_id=next_id)
            env.process(new_node.run())
            
            # Choose a random existing node to bootstrap
            bootstrap = random.choice(nodes)
            
            # Join the ring
            env.process(new_node.join(bootstrap))
            
            # Add to our node list
            nodes.append(new_node)
            next_id += 1
    
    # Process to remove nodes periodically
    def node_remover():
        yield env.timeout(30)  # Wait for some nodes to join
        
        while True:
            yield env.timeout(random.randint(10, 20))
            
            # Only remove if we have enough nodes
            if len(nodes) > 3:
                # Choose a random node to remove (not the first node)
                node = random.choice(nodes[1:])
                node.leave()
                nodes.remove(node)
    
    # Process to send ping messages
    def ping_sender():
        yield env.timeout(15)  # Wait for the network to form
        
        while True:
            yield env.timeout(random.randint(5, 10))
            
            # Only send if we have at least 2 nodes
            if len(nodes) >= 2:
                sender = random.choice(nodes)
                target = random.choice([n for n in nodes if n != sender])
                
                print(f"{env.now:.1f}: {sender} pinging {target}")
                sender.send_message(target, 'PING')
    
    # Start the processes
    env.process(node_creator())
    env.process(node_remover())
    env.process(ping_sender())
    
    # Run the simulation
    env.run(until=duration)
    
    # Print final ring structure
    print("\nFinal ring structure:")
    if nodes:
        curr = nodes[0]
        start_id = curr.node_id
        visited = set()
        
        while curr.node_id not in visited:
            print(f"  {curr} -> {curr.right_neighbor}")
            visited.add(curr.node_id)
            curr = curr.right_neighbor
            
            # Stop if we've gone all the way around
            if curr.node_id == start_id:
                break
    
    print(f"Simulation ended with {len(nodes)} active nodes")

def run_storage_simulation(env, max_nodes=10, duration=150):
    """Run a DHT simulation with storage capabilities"""
    print("Starting storage DHT simulation...")
    
    # Create first node
    first_node = StorageNode(env, node_id=0)
    env.process(first_node.run())
    
    nodes = [first_node]
    next_data_id = 0
    
    # Process to add new nodes periodically
    def node_creator():
        next_id = 1
        while next_id < max_nodes:
            yield env.timeout(random.randint(3, 8))
            
            # Create a new node
            new_node = StorageNode(env, node_id=next_id)
            env.process(new_node.run())
            
            # Choose a random existing node to bootstrap
            bootstrap = random.choice(nodes)
            
            # Join the ring
            env.process(new_node.join(bootstrap))
            
            # Add to our node list
            nodes.append(new_node)
            next_id += 1
    
    # Process to remove nodes periodically
    def node_remover():
        yield env.timeout(40)  # Wait for some nodes to join
        
        while True:
            yield env.timeout(random.randint(15, 25))
            
            # Only remove if we have enough nodes
            if len(nodes) > 3:
                # Choose a random node to remove (not the first node)
                node = random.choice(nodes[1:])
                node.leave()
                nodes.remove(node)
    
    # Process to store data periodically
    def data_storer():
        nonlocal next_data_id
        yield env.timeout(20)  # Wait for the network to form
        
        while True:
            yield env.timeout(random.randint(5, 10))
            
            if nodes:
                # Choose a random node
                node = random.choice(nodes)
                
                # Create a key-value pair
                key = f"key-{next_data_id}"
                value = f"value-{next_data_id}"
                next_data_id += 1
                
                # Store the data
                print(f"{env.now:.1f}: Storing {key}={value} via {node}")
                node.store(key, value)
    
    # Process to retrieve data periodically
    def data_retriever():
        yield env.timeout(30)  # Wait for some data to be stored
        
        while True:
            yield env.timeout(random.randint(7, 12))
            
            if nodes and next_data_id > 0:
                # Choose a random node
                node = random.choice(nodes)
                
                # Choose a random existing key
                key_id = random.randint(0, next_data_id - 1)
                key = f"key-{key_id}"
                
                # Retrieve the data
                print(f"{env.now:.1f}: Retrieving {key} via {node}")
                node.send_message(node, 'RETRIEVE', {'key': key, 'origin': node})
    
    # Start the processes
    env.process(node_creator())
    env.process(node_remover())
    env.process(data_storer())
    env.process(data_retriever())
    
    # Run the simulation
    env.run(until=duration)
    
    # Print data storage statistics
    print("\nData storage statistics:")
    total_primary = sum(len(node.data) for node in nodes)
    total_replicas = sum(len(node.replicas) for node in nodes)
    
    print(f"Total data items: {total_primary} primary, {total_replicas} replicated")
    for node in nodes:
        print(f"  {node}: {len(node.data)} primary items, {len(node.replicas)} replicated items")

def run_advanced_simulation(env, max_nodes=15, duration=200):
    """Run a DHT simulation with advanced routing"""
    print("Starting advanced DHT simulation...")
    
    # Create first node
    first_node = AdvancedNode(env, node_id=0)
    env.process(first_node.run())
    
    nodes = [first_node]
    next_data_id = 0
    
    # Process to add new nodes periodically
    def node_creator():
        next_id = 1
        while next_id < max_nodes:
            yield env.timeout(random.randint(3, 8))
            
            # Create a new node
            new_node = AdvancedNode(env, node_id=next_id)
            env.process(new_node.run())
            
            # Choose a random existing node to bootstrap
            bootstrap = random.choice(nodes)
            
            # Join the ring
            env.process(new_node.join(bootstrap))
            
            # Add to our node list
            nodes.append(new_node)
            next_id += 1
    
    # Process to remove nodes periodically
    def node_remover():
        yield env.timeout(50)  # Wait for some nodes to join
        
        while True:
            yield env.timeout(random.randint(20, 30))
            
            # Only remove if we have enough nodes
            if len(nodes) > 3:
                # Choose a random node to remove (not the first node)
                node = random.choice(nodes[1:])
                node.leave()
                nodes.remove(node)
    
    # Process to store data periodically
    def data_storer():
        nonlocal next_data_id
        yield env.timeout(25)  # Wait for the network to form
        
        while True:
            yield env.timeout(random.randint(8, 15))
            
            if nodes:
                # Choose a random node
                node = random.choice(nodes)
                
                # Create a key-value pair
                key = f"key-{next_data_id}"
                value = f"value-{next_data_id}"
                next_data_id += 1
                
                # Store the data
                print(f"{env.now:.1f}: Storing {key}={value} via {node}")
                node.store(key, value)
    
    # Process to test advanced routing
    def route_tester():
        yield env.timeout(60)  # Wait for the network and long links to form
        
        # Run several routing tests
        for i in range(5):
            yield env.timeout(random.randint(10, 20))
            
            if len(nodes) > 2:
                # Choose random source and target
                source = random.choice(nodes)
                
                # Generate a random target ID (may not be an actual node)
                target_id = random.randint(0, 99)
                
                # Create a test message
                test_message = {
                    'text': f"Test message {i}",
                    'hops': 0,
                    'source': source
                }
                
                # Send the message
                print(f"{env.now:.1f}: Routing test message from {source} to node ID {target_id}")
                source.route_message(target_id, test_message)
    
    # Start the processes
    env.process(node_creator())
    env.process(node_remover())
    env.process(data_storer())
    env.process(route_tester())
    
    # Run the simulation
    env.run(until=duration)
    
    # Print statistics
    print("\nAdvanced routing statistics:")
    total_long_links = sum(len(node.long_links) for node in nodes)
    avg_long_links = total_long_links / len(nodes) if nodes else 0
    
    print(f"Total nodes: {len(nodes)}")
    print(f"Total long links: {total_long_links} (average: {avg_long_links:.2f} per node)")
    
    for node in nodes:
        print(f"  {node}: {len(node.long_links)} long links")

def main():
    """Entry point for the DHT simulator"""
    parser = argparse.ArgumentParser(description="DHT Simulator")
    parser.add_argument("--demo", type=str, choices=["basic", "storage", "advanced"], 
                        default="basic", help="Simulation complexity level")
    parser.add_argument("--nodes", type=int, default=10, 
                        help="Maximum number of nodes")
    parser.add_argument("--duration", type=int, default=100, 
                        help="Simulation duration")
    parser.add_argument("--seed", type=int, default=None, 
                        help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)
    
    # Print simulation parameters
    print(f"\n{'='*50}")
    print(f"DHT Simulation with {args.demo.upper()} complexity")
    print(f"Duration: {args.duration} time units")
    print(f"Max nodes: {args.nodes}")
    print(f"Random seed: {args.seed if args.seed else 'None (random)'}")
    print(f"{'='*50}\n")
    
    # Create the simulation environment
    env = simpy.Environment()
    
    # Run the appropriate simulation
    try:
        if args.demo == "basic":
            run_basic_simulation(env, args.nodes, args.duration)
        elif args.demo == "storage":
            run_storage_simulation(env, args.nodes, args.duration)
        elif args.demo == "advanced":
            run_advanced_simulation(env, args.nodes, args.duration)
    except KeyboardInterrupt:
        print("\nSimulation interrupted by user")
    except Exception as e:
        print(f"\nError during simulation: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
    print("\nSimulation completed successfully")