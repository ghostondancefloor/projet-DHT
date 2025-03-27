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

from version1.Node import Node
from version1.StorageNode import StorageNode
from version1.AdvancedNode import AdvancedNode


class DemoLevel(Enum):
    BASIC = "basic"       # Just a ring with node join/leave
    STORAGE = "storage"   # Adding storage capabilities
    ADVANCED = "advanced" # Adding advanced routing


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
        ring_path = []
        
        while curr.node_id not in visited:
            ring_path.append(curr.node_id)
            visited.add(curr.node_id)
            print(f"  {curr} -> {curr.right_neighbor}")
            curr = curr.right_neighbor
            
            # Stop if we've gone all the way around
            if curr.node_id == start_id:
                break
        
        # Visualize the ring structure
        print("\nRing visualization:")
        if len(ring_path) > 0:
            # Create a circular representation
            ring_str = " → ".join(str(n) for n in ring_path)
            if curr.node_id == start_id:
                ring_str += f" → {start_id} (complete ring)"
            else:
                ring_str += " (incomplete ring)"
            print(ring_str)
            
            # Print node distribution along the ID space
            print("\nNode distribution in ID space (0-99):")
            id_line = ["·"] * 100
            for node_id in ring_path:
                id_line[node_id] = "N"
            
            # Print in chunks of 20 for readability
            for i in range(0, 100, 20):
                chunk = "".join(id_line[i:i+20])
                markers = "".join([str(i+j)[0] if j % 5 == 0 else " " for j in range(20)])
                print(f"{i:2d}: {chunk} {i+19}")
                print(f"    {markers}")
            
            print("\nN = Node location, · = Empty ID space")
    
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