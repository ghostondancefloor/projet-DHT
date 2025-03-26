"""
DHT Simulator
========================================
Simulates a Distributed Hash Table (DHT) with basic, storage, and advanced routing capabilities.
Includes visualization of the DHT ring at the end of simulation.
"""

import simpy
import random
import hashlib
import argparse
from enum import Enum
import matplotlib.pyplot as plt
import math
import numpy as np

# --------------------------- DHT NODES TYPES --------------------------- #

from Node import Node
from StorageNode import StorageNode
from AdvancedNode import AdvancedNode


# --------------------------- ENUM CONFIG --------------------------- #

class DemoLevel(Enum):
    BASIC = "basic"
    STORAGE = "storage"
    ADVANCED = "advanced"


# --------------------------- VISUALIZATION --------------------------- #

def visualize_dht_ring(nodes, demo_level):
    """
    Visualize the DHT ring with all nodes and their connections.
    """
    plt.figure(figsize=(10, 10))
    
    # Create a circle
    circle = plt.Circle((0, 0), 1, fill=False, color='black', linestyle='--')
    plt.gca().add_patch(circle)
    
    # Calculate positions for each node based on their ID
    node_positions = {}
    for node in nodes:
        angle = 2 * math.pi * node.node_id / 100  # Normalize to [0, 2π]
        x = math.cos(angle)
        y = math.sin(angle)
        node_positions[node] = (x, y)
    
    # Draw nodes
    for node, (x, y) in node_positions.items():
        color = 'blue'
        if isinstance(node, AdvancedNode):
            color = 'red'
        elif isinstance(node, StorageNode):
            color = 'green'
            
        plt.plot(x, y, 'o', markersize=10, color=color)
        plt.text(x*1.1, y*1.1, str(node.node_id), fontsize=9)
    
    # Draw connections (right neighbor)
    for node in nodes:
        if node.right_neighbor != node:  # Skip self-connections
            start_pos = node_positions[node]
            end_pos = node_positions[node.right_neighbor]
            plt.arrow(start_pos[0], start_pos[1], 
                      (end_pos[0] - start_pos[0])*0.9, 
                      (end_pos[1] - start_pos[1])*0.9,
                      head_width=0.05, head_length=0.1, fc='black', ec='black')
    
    # Draw long links for advanced nodes
    if demo_level == 'advanced':
        for node in nodes:
            if isinstance(node, AdvancedNode):
                for target_id, target_node in node.long_links.items():
                    if target_node != node.right_neighbor and target_node != node.left_neighbor:
                        start_pos = node_positions[node]
                        end_pos = node_positions[target_node]
                        plt.arrow(start_pos[0], start_pos[1], 
                                  (end_pos[0] - start_pos[0])*0.9, 
                                  (end_pos[1] - start_pos[1])*0.9,
                                  head_width=0.05, head_length=0.1, fc='purple', ec='purple',
                                  linestyle='--')
    
    # Add legend
    legend_items = []
    if demo_level == 'basic':
        legend_items.append(plt.plot([], [], 'o', color='blue', label='Basic Node')[0])
    elif demo_level == 'storage':
        legend_items.append(plt.plot([], [], 'o', color='green', label='Storage Node')[0])
    elif demo_level == 'advanced':
        legend_items.append(plt.plot([], [], 'o', color='red', label='Advanced Node')[0])
        legend_items.append(plt.Line2D([0], [0], color='purple', linestyle='--', 
                          marker='>', markersize=8, label='Long Link'))
    
    legend_items.append(plt.Line2D([0], [0], color='black', marker='>', 
                      markersize=8, label='Right Neighbor'))
    
    plt.legend(handles=legend_items, loc='upper right')
    
    # Set plot limits and labels
    plt.xlim(-1.5, 1.5)
    plt.ylim(-1.5, 1.5)
    title = f"DHT Ring Visualization - {demo_level.upper()} Mode"
    plt.title(title)
    plt.gca().set_aspect('equal')
    plt.grid(True)
    
    # Add ID circle markers
    for i in range(0, 100, 10):
        angle = 2 * math.pi * i / 100
        x = 1.2 * math.cos(angle)
        y = 1.2 * math.sin(angle)
        plt.text(x, y, str(i), fontsize=8, ha='center', va='center',
                color='gray', alpha=0.7)
    
    # Add data visualization for storage nodes
    if demo_level in ['storage', 'advanced']:
        data_text = []
        for node in nodes:
            if isinstance(node, StorageNode) and node.data:
                data_items = ", ".join([f"{k}={v}" for k, v in node.data.items()])
                data_text.append(f"Node {node.node_id}: {data_items}")
        
        if data_text:
            plt.figtext(0.5, 0.02, "\n".join(data_text), ha='center', fontsize=8, 
                       bbox=dict(facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig(f"dht_ring_{demo_level}.png")
    print(f"Visualization saved as 'dht_ring_{demo_level}.png'")
    plt.show()

# --------------------------- MAIN SIMULATION ENTRY --------------------------- #

def run_basic_simulation(env, num_nodes):
    """Run a basic DHT simulation with simple nodes."""
    print("Running BASIC simulation...")
    
    # Create nodes with random IDs
    nodes = []
    for _ in range(num_nodes):
        node_id = random.randint(0, 99)
        node = Node(env, node_id)
        nodes.append(node)
        env.process(node.run())
    
    # First node is the bootstrap node
    bootstrap = nodes[0]
    
    # Other nodes join the network through the bootstrap
    for node in nodes[1:]:
        env.process(node.join(bootstrap))
        yield env.timeout(1)  # Wait for the join process to complete
    
    # Let the network stabilize
    yield env.timeout(5)
    
    # Perform some ping operations
    for _ in range(3):
        sender = random.choice(nodes)
        receiver = random.choice(nodes)
        if sender != receiver:
            print(f"{env.now:.1f}: {sender} pings {receiver}")
            sender.send_message(receiver, 'PING')
            yield env.timeout(1)
    
    # Simulate a node leaving
    leaving_node = nodes[random.randint(1, len(nodes)-1)]
    leaving_node.leave()
    nodes.remove(leaving_node)
    
    # Let the network stabilize again
    yield env.timeout(5)
    
    return nodes

def run_storage_simulation(env, num_nodes):
    """Run a storage DHT simulation with nodes that can store and retrieve data."""
    print("Running STORAGE simulation...")
    
    # Create storage nodes with random IDs
    nodes = []
    for _ in range(num_nodes):
        node_id = random.randint(0, 99)
        node = StorageNode(env, node_id)
        nodes.append(node)
        env.process(node.run())
    
    # First node is the bootstrap node
    bootstrap = nodes[0]
    
    # Other nodes join the network through the bootstrap
    for node in nodes[1:]:
        env.process(node.join(bootstrap))
        yield env.timeout(1)  # Wait for the join process to complete
    
    # Let the network stabilize
    yield env.timeout(5)
    
    # Store some data
    for i in range(5):
        node = random.choice(nodes)
        key = f"key{i}"
        value = f"value{i}"
        print(f"{env.now:.1f}: {node} initiates storage of {key}={value}")
        node.store(key, value)
        yield env.timeout(2)
    
    # Simulate a node leaving
    leaving_node = nodes[random.randint(1, len(nodes)-1)]
    leaving_node.leave()
    nodes.remove(leaving_node)
    
    # Let the network stabilize again
    yield env.timeout(5)
    
    # Store some more data
    for i in range(5, 8):
        node = random.choice(nodes)
        key = f"key{i}"
        value = f"value{i}"
        print(f"{env.now:.1f}: {node} initiates storage of {key}={value}")
        node.store(key, value)
        yield env.timeout(2)
    
    return nodes

def run_advanced_simulation(env, num_nodes):
    """Run an advanced DHT simulation with nodes that have long links for efficient routing."""
    print("Running ADVANCED simulation...")
    
    # Create advanced nodes with random IDs
    nodes = []
    for _ in range(num_nodes):
        node_id = random.randint(0, 99)
        node = AdvancedNode(env, node_id)
        nodes.append(node)
        env.process(node.run())
    
    # First node is the bootstrap node
    bootstrap = nodes[0]
    
    # Other nodes join the network through the bootstrap
    for node in nodes[1:]:
        env.process(node.join(bootstrap))
        yield env.timeout(1)  # Wait for the join process to complete
    
    # Let the network stabilize
    yield env.timeout(10)
    
    # Store some data
    for i in range(5):
        node = random.choice(nodes)
        key = f"key{i}"
        value = f"value{i}"
        print(f"{env.now:.1f}: {node} initiates storage of {key}={value}")
        node.store(key, value)
        yield env.timeout(2)
    
    # Let the network stabilize again
    yield env.timeout(5)
    
    # Send some routed messages
    for i in range(3):
        sender = random.choice(nodes)
        target_id = random.randint(0, 99)
        message = f"Message {i} from {sender} to {target_id}"
        print(f"{env.now:.1f}: {sender} sends routed message to {target_id}: {message}")
        sender.route_message(target_id, message)
        yield env.timeout(3)
    
    return nodes

def run_basic_simulation(env, num_nodes):
    """Run a basic DHT simulation with simple nodes."""
    print("Running BASIC simulation...")
    
    # Create nodes with random IDs
    nodes = []
    for _ in range(num_nodes):
        node_id = random.randint(0, 99)
        node = Node(env, node_id)
        nodes.append(node)
        env.process(node.run())
    
    # First node is the bootstrap node
    bootstrap = nodes[0]
    
    # Other nodes join the network through the bootstrap
    for node in nodes[1:]:
        env.process(node.join(bootstrap))
        yield env.timeout(1)  # Wait for the join process to complete
    
    # Let the network stabilize
    yield env.timeout(5)
    
    # Perform some ping operations
    for _ in range(3):
        sender = random.choice(nodes)
        receiver = random.choice(nodes)
        if sender != receiver:
            print(f"{env.now:.1f}: {sender} pings {receiver}")
            sender.send_message(receiver, 'PING')
            yield env.timeout(1)
    
    # Simulate a node leaving
    leaving_node = nodes[random.randint(1, len(nodes)-1)]
    leaving_node.leave()
    nodes.remove(leaving_node)
    
    # Let the network stabilize again
    yield env.timeout(5)
    
    # Return the nodes for visualization
    return nodes

def run_storage_simulation(env, num_nodes):
    """Run a storage DHT simulation with nodes that can store and retrieve data."""
    print("Running STORAGE simulation...")
    
    # Create storage nodes with random IDs
    nodes = []
    for _ in range(num_nodes):
        node_id = random.randint(0, 99)
        node = StorageNode(env, node_id)
        nodes.append(node)
        env.process(node.run())
    
    # First node is the bootstrap node
    bootstrap = nodes[0]
    
    # Other nodes join the network through the bootstrap
    for node in nodes[1:]:
        env.process(node.join(bootstrap))
        yield env.timeout(1)  # Wait for the join process to complete
    
    # Let the network stabilize
    yield env.timeout(5)
    
    # Store some data
    for i in range(5):
        node = random.choice(nodes)
        key = f"key{i}"
        value = f"value{i}"
        print(f"{env.now:.1f}: {node} initiates storage of {key}={value}")
        node.store(key, value)
        yield env.timeout(2)
    
    # Simulate a node leaving
    leaving_node = nodes[random.randint(1, len(nodes)-1)]
    leaving_node.leave()
    nodes.remove(leaving_node)
    
    # Let the network stabilize again
    yield env.timeout(5)
    
    # Store some more data
    for i in range(5, 8):
        node = random.choice(nodes)
        key = f"key{i}"
        value = f"value{i}"
        print(f"{env.now:.1f}: {node} initiates storage of {key}={value}")
        node.store(key, value)
        yield env.timeout(2)
    
    # Return the nodes for visualization
    return nodes

def run_advanced_simulation(env, num_nodes):
    """Run an advanced DHT simulation with nodes that have long links for efficient routing."""
    print("Running ADVANCED simulation...")
    
    # Create advanced nodes with random IDs
    nodes = []
    for _ in range(num_nodes):
        node_id = random.randint(0, 99)
        node = AdvancedNode(env, node_id)
        nodes.append(node)
        env.process(node.run())
    
    # First node is the bootstrap node
    bootstrap = nodes[0]
    
    # Other nodes join the network through the bootstrap
    for node in nodes[1:]:
        env.process(node.join(bootstrap))
        yield env.timeout(1)  # Wait for the join process to complete
    
    # Let the network stabilize
    yield env.timeout(10)
    
    # Store some data
    for i in range(5):
        node = random.choice(nodes)
        key = f"key{i}"
        value = f"value{i}"
        print(f"{env.now:.1f}: {node} initiates storage of {key}={value}")
        node.store(key, value)
        yield env.timeout(2)
    
    # Let the network stabilize again
    yield env.timeout(5)
    
    # Send some routed messages
    for i in range(3):
        sender = random.choice(nodes)
        target_id = random.randint(0, 99)
        message = f"Message {i} from {sender} to {target_id}"
        print(f"{env.now:.1f}: {sender} sends routed message to {target_id}: {message}")
        sender.route_message(target_id, message)
        yield env.timeout(3)
    
    # Return the nodes for visualization
    return nodes

def main():
    parser = argparse.ArgumentParser(description="DHT Simulator")
    parser.add_argument("--mode", choices=["basic", "storage", "advanced"], default="basic", 
                        help="Simulation mode (default: basic)")
    parser.add_argument("--nodes", type=int, default=10, 
                        help="Number of nodes in the simulation (default: 10)")
    parser.add_argument("--time", type=int, default=60, 
                        help="Simulation time in seconds (default: 60)")
    parser.add_argument("--seed", type=int, default=None, 
                        help="Random seed for reproducibility")
    args = parser.parse_args()
    
    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)
    
    # Create SimPy environment
    env = simpy.Environment()
    
    # Store all nodes in a global list
    all_nodes = []
    
    # Run the appropriate simulation based on the mode
    if args.mode == "basic":
        demo_level = DemoLevel.BASIC
        simulation = run_basic_simulation(env, args.nodes)
    elif args.mode == "storage":
        demo_level = DemoLevel.STORAGE
        simulation = run_storage_simulation(env, args.nodes)
    elif args.mode == "advanced":
        demo_level = DemoLevel.ADVANCED
        simulation = run_advanced_simulation(env, args.nodes)
    
    # Keep a reference to the generator
    sim_generator = env.process(simulation)
    
    # Create a callback to store the nodes when the simulation ends
    def store_nodes(event):
        global final_nodes
        final_nodes = event.value
    
    # Add a callback to the simulation completion
    sim_generator.callbacks.append(store_nodes)
    
    # Define a global variable to store the final nodes
    global final_nodes
    final_nodes = []
    
    # Run the simulation until the specified time
    env.run(until=args.time)
    
    # If the simulation didn't complete, extract the nodes from the generator
    if not final_nodes:
        # We need to take the nodes from the generator's frame
        import inspect
        frame = inspect.currentframe()
        for frame_info in inspect.getouterframes(frame):
            if 'simulation' in frame_info.frame.f_locals:
                gen = frame_info.frame.f_locals['simulation']
                if hasattr(gen, 'gi_frame') and gen.gi_frame is not None:
                    if 'nodes' in gen.gi_frame.f_locals:
                        final_nodes = gen.gi_frame.f_locals['nodes']
                        break
    
    # Visualize the final DHT ring
    if final_nodes:
        visualize_dht_ring(final_nodes, demo_level.value)
    else:
        print("Error: Could not extract nodes for visualization.")
    
    print(f"Simulation completed after {env.now:.1f} time units")

if __name__ == "__main__":
    main()
