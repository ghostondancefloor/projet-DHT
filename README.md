# DHT Simulator – Python Simulation of Distributed Hash Tables

A powerful and visual simulator of **Distributed Hash Tables (DHT)** implemented in Python using [SimPy](https://simpy.readthedocs.io/), supporting three simulation modes:
- **Basic ring topology**
- **Storage-enabled nodes**
- **Advanced routing with long links**

> Includes a fully-featured visualization of the DHT ring with matplotlib.

---

## 📦 Features

- ✅ Simulates peer-to-peer DHT protocols
- ✅ Dynamic **join/leave** of nodes
- ✅ **Key-value storage and replication**
- ✅ Advanced routing with **shortcut links** (similar to finger tables)
- ✅ **Visual ring topology rendering** in PNG format
- ✅ Customizable via command-line options

---

## 🛠️ Architecture

The simulation is modular, with each node type in a separate file:

- `Node.py` – Base class with neighbor management and message handling
- `StorageNode.py` – Inherits from Node, adds key-value storage and replication
- `AdvancedNode.py` – Inherits from StorageNode, adds advanced routing and long links
- `dht-simulationv2.py` – Main simulation logic and CLI interface

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/ghostondancefloor/projet-DHT.git
cd projet-DHT

