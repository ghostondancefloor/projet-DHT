#!/usr/bin/env python3
"""
Full DHT Simulator (Single File Version)
========================================
Simulates a Distributed Hash Table (DHT) with basic, storage, and advanced routing capabilities.
"""

import simpy
import random
import hashlib
import argparse
from enum import Enum

from version1.Node import Node
from version1.StorageNode import StorageNode
from version1.AdvancedNode import AdvancedNode

# --------------------------- ENUM CONFIG --------------------------- #

class DemoLevel(Enum):
    BASIC = "basic"
    STORAGE = "storage"
    ADVANCED = "advanced"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--demo', type=str, choices=['basic', 'storage', 'advanced'], default='basic')
    parser.add_argument('--nodes', type=int, default=10)
    parser.add_argument('--duration', type=int, default=100)
    args = parser.parse_args()

    print("\nDHT Simulation")
    print("==============")
    print(f"Demo Level : {args.demo.upper()}\nDuration    : {args.duration}\nNodes       : {args.nodes}\n")

    env = simpy.Environment()
    if args.demo == 'basic':
        run_demo(env, Node, args.nodes, args.duration)
    elif args.demo == 'storage':
        run_demo(env, StorageNode, args.nodes, args.duration)
    elif args.demo == 'advanced':
        run_demo(env, AdvancedNode, args.nodes, args.duration)


def run_demo(env, NodeClass, max_nodes, duration):
    nodes = []
    first_node = NodeClass(env, 0)
    env.process(first_node.run())
    nodes.append(first_node)

    def creator():
        for i in range(1, max_nodes):
            yield env.timeout(random.randint(3, 6))
            new_node = NodeClass(env, i)
            env.process(new_node.run())
            bootstrap = random.choice(nodes)
            env.process(new_node.join(bootstrap))
            nodes.append(new_node)

    def storer():
        yield env.timeout(15)
        for i in range(5):
            yield env.timeout(10)
            node = random.choice(nodes)
            key, value = f"key-{i}", f"val-{i}"
            if hasattr(node, 'store'):
                node.store(key, value)

    def router():
        yield env.timeout(40)
        for i in range(3):
            yield env.timeout(12)
            node = random.choice(nodes)
            tid = random.randint(0, 99)
            if hasattr(node, 'route_message'):
                node.route_message(tid, f"Hello to {tid}!")

    env.process(creator())
    env.process(storer())
    env.process(router())
    env.run(until=duration)

if __name__ == '__main__':
    main()