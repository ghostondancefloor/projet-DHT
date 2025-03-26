"""
DHT Simulator Launcher
======================
This script launches DHT simulations with different complexity levels.
"""

import argparse
import random
import simpy
import sys
from enum import Enum

# Import our DHT implementation files
try:
    from Node import Node
    from StorageNode import StorageNode
    from AdvancedNode import AdvancedNode
except ImportError:
    print("Error: Unable to import DHT implementation files.")
    print("Make sure Node.py, StorageNode.py, and AdvancedNode.py are in the current directory.")
    sys.exit(1)

class DemoLevel(Enum):
    BASIC = "basic"  # Simple ring with basic nodes
    STORAGE = "storage"  # Ring with storage nodes
    ADVANCED = "advanced"  # Ring with advanced routing
    FULL = "full"  # Full featured DHT with advanced routing and global optimization

def run_basic_simulation(env, duration=100, max_nodes=10):
    """Run a basic ring simulation"""
    print("Starting basic ring simulation...")
    
    # Create the first node
    first_node = Node(env, node_id=0)
    env.process(first_node.run())
    nodes = [first_node]
    
    # Process to add nodes periodically
    def node_creator():
        next_node_id = 1
        while next_node_id < max_nodes:
            yield env.timeout(random.randint(5, 15))
            
            # Choose a random bootstrap node
            bootstrap_node = random.choice(nodes)
            
            # Create and start the new node
            new_node = Node(env, node_id=next_node_id)
            env.process(new_node.run())
            
            # Join the ring
            env.process(new_node.join(bootstrap_node))
            nodes.append(new_node)
            
            next_node_id += 1
    
    # Process to remove nodes periodically
    def node_remover():
        yield env.timeout(30)  # Wait for some nodes to join
        while True:
            yield env.timeout(random.randint(20, 30))
            if len(nodes) > 3:  # Keep at least a few nodes
                node = random.choice(nodes[1:])  # Don't remove the initial node
                nodes.remove(node)
                node.leave()
    
    # Process to send messages around the ring
    def message_sender():
        yield env.timeout(10)  # Wait for the network to form
        while True:
            yield env.timeout(random.randint(5, 15))
            if len(nodes) > 1:
                sender = random.choice(nodes)
                receiver = random.choice(nodes)
                if sender != receiver:
                    print(f"{env.now:.1f}: {sender} sending message to {receiver}")
                    sender.send_message(receiver, 'MSG', f"Hello from {sender}!")
    
    # Start the processes
    env.process(node_creator())
    env.process(node_remover())
    env.process(message_sender())
    
    # Run the simulation
    env.run(until=duration)
    
    # Print statistics
    print(f"\nSimulation completed with {len(nodes)} active nodes")
    print("Ring structure:")
    current = nodes[0]
    start_id = current.node_id
    visited = [current]
    print(f"  {current} -> {current.right_neighbor}")
    current = current.right_neighbor
    
    while current.node_id != start_id and len(visited) < len(nodes):
        print(f"  {current} -> {current.right_neighbor}")
        visited.append(current)
        current = current.right_neighbor
    
    if len(visited) != len(nodes):
        print(f"Warning: Only visited {len(visited)} of {len(nodes)} nodes - ring may be broken")

def run_storage_simulation(env, duration=200, max_nodes=10):
    """Run a storage ring simulation"""
    print("Starting storage ring simulation...")
    
    # Create the first node
    first_node = StorageNode(env, node_id=0)
    env.process(first_node.run())
    nodes = [first_node]
    
    # Track the number of data items created
    next_data_id = 0
    
    # Process to add nodes periodically
    def node_creator():
        next_node_id = 1
        while next_node_id < max_nodes:
            yield env.timeout(random.randint(5, 15))
            
            # Choose a random bootstrap node
            bootstrap_node = random.choice(nodes)
            
            # Create and start the new node
            new_node = StorageNode(env, node_id=next_node_id)
            env.process(new_node.run())
            
            # Join the ring
            env.process(new_node.join(bootstrap_node))
            nodes.append(new_node)
            
            next_node_id += 1
    
    # Process to remove nodes periodically
    def node_remover():
        yield env.timeout(40)  # Wait for some nodes to join
        while True:
            yield env.timeout(random.randint(30, 50))
            if len(nodes) > 3:  # Keep at least a few nodes
                node = random.choice(nodes[1:])  # Don't remove the initial node
                nodes.remove(node)
                node.leave()
    
    # Process to create data items
    def data_creator():
        nonlocal next_data_id
        yield env.timeout(20)  # Wait for the network to form
        while True:
            yield env.timeout(random.randint(8, 15))
            if nodes:
                # Choose a random node
                node = random.choice(nodes)
                key = f"key-{next_data_id}"
                value = f"value-{next_data_id}"
                
                # Store data
                node.store_data(key, value)
                next_data_id += 1
    
    # Process to retrieve data items
    def data_retriever():
        nonlocal next_data_id
        yield env.timeout(40)  # Wait for some data to be stored
        while True:
            yield env.timeout(random.randint(10, 20))
            if nodes and next_data_id > 0:
                # Choose a random node
                node = random.choice(nodes)
                # Choose a random existing key
                key = f"key-{random.randint(0, max(0, next_data_id - 1))}"
                
                # Get data
                print(f"{env.now:.1f}: Requesting data {key} from {node}")
                node.send_message(node, 'GET_REQUEST', {'key': key})
    
    # Start the processes
    env.process(node_creator())
    env.process(node_remover())
    env.process(data_creator())
    env.process(data_retriever())
    
    # Run the simulation
    env.run(until=duration)
    
    # Print statistics
    print(f"\nSimulation completed with {len(nodes)} active nodes")
    total_data = sum(len(node.data_store) for node in nodes)
    print(f"Total data items stored: {total_data}")
    for node in nodes:
        print(f"  {node}: {len(node.data_store)} data items")

def run_advanced_simulation(env, duration=300, max_nodes=15):
    """Run an advanced ring simulation with long links"""
    print("Starting advanced ring simulation...")
    
    # Create the first node
    first_node = AdvancedNode(env, node_id=0)
    env.process(first_node.run())
    nodes = [first_node]
    
    # Process to add nodes periodically
    def node_creator():
        next_node_id = 1
        while next_node_id < max_nodes:
            yield env.timeout(random.randint(5, 15))
            
            # Choose a random bootstrap node
            bootstrap_node = random.choice(nodes)
            
            # Create and start the new node
            new_node = AdvancedNode(env, node_id=next_node_id)
            env.process(new_node.run())
            
            # Join the ring
            env.process(new_node.join(bootstrap_node))
            nodes.append(new_node)
            
            next_node_id += 1
    
    # Process to create long links between nodes
    def long_link_creator():
        yield env.timeout(30)  # Wait for the network to stabilize
        
        # Create some long links between distant nodes
        for node in nodes:
            # Create 2-3 long links per node
            for _ in range(random.randint(2, 3)):
                # Try to find a node that's not a direct neighbor
                candidates = [n for n in nodes if n != node and n != node.left_neighbor and n != node.right_neighbor]
                if candidates:
                    target = random.choice(candidates)
                    node.send_message(target, 'LONG_LINK_REQUEST')
            
            yield env.timeout(2)  # Add a small delay between nodes
    
    # Process to test routing
    def router():
        yield env.timeout(60)  # Wait for long links to be established
        
        # Run several routing tests
        for _ in range(10):
            yield env.timeout(random.randint(10, 20))
            
            if len(nodes) > 3:
                # Pick random source and target nodes
                source = random.choice(nodes)
                target_id = random.randint(0, 99)  # May not be an actual node ID
                
                # Create the routing message
                message = f"Test routing message {random.randint(1000, 9999)}"
                
                # Try to route to the target
                print(f"{env.now:.1f}: Routing message from {source} to node with ID {target_id}")
                next_hop = source.find_best_route(target_id)
                source.send_message(next_hop, 'ROUTE_REQUEST', {
                    'target_id': target_id,
                    'data': message,
                    'hops': 0
                })
    
    # Start the processes
    env.process(node_creator())
    env.process(long_link_creator())
    env.process(router())
    
    # Run the simulation
    env.run(until=duration)
    
    # Print statistics
    print(f"\nSimulation completed with {len(nodes)} active nodes")
    
    # Count long links
    total_long_links = 0
    for node in nodes:
        num_long_links = len(node.long_links)
        total_long_links += num_long_links
        print(f"  {node}: {num_long_links} long links")
    
    print(f"Average long links per node: {total_long_links/len(nodes) if nodes else 0:.2f}")

def main():
    """Parse arguments and run the appropriate simulation"""
    parser = argparse.ArgumentParser(description='Simple DHT Simulator')
    parser.add_argument('--demo', type=str, default='basic',
                        choices=['basic', 'storage', 'advanced'],
                        help='Demonstration level (basic, storage, advanced)')
    parser.add_argument('--duration', type=int, default=200,
                        help='Simulation duration')
    parser.add_argument('--nodes', type=int, default=10,
                        help='Maximum number of nodes')
    parser.add_argument('--seed', type=int, default=None,
                        help='Random seed for reproducibility')
    
    args = parser.parse_args()
    
    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)
    
    # Create simulation environment
    env = simpy.Environment()
    
    # Print simulation parameters
    print(f"\n{'='*50}")
    print(f"DHT Simulation with {args.demo.upper()} complexity")
    print(f"Duration: {args.duration} time units")
    print(f"Max nodes: {args.nodes}")
    print(f"Random seed: {args.seed if args.seed else 'None (random)'}")
    print(f"{'='*50}\n")
    
    # Run the appropriate simulation
    try:
        if args.demo == 'basic':
            run_basic_simulation(env, args.duration, args.nodes)
        elif args.demo == 'storage':
            run_storage_simulation(env, args.duration, args.nodes)
        elif args.demo == 'advanced':
            run_advanced_simulation(env, args.duration, args.nodes)
    except KeyboardInterrupt:
        print("\nSimulation interrupted by user")
    except Exception as e:
        print(f"\nError during simulation: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
    print("\nSimulation completed")