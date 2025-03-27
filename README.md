# Distributed Hash Table (DHT) Simulation en Python

Ce projet simule un réseau **DHT (Distributed Hash Table)** dynamique utilisant le module `SimPy`. Il couvre les concepts de base des DHT, l'intégration du stockage de données, et une version avancée de routage avec table des doigts et liens longs inspirés de **Chord**.

---

## 🔧 Fichiers du projet

| Fichier | Description |
|--------|-------------|
| `dht_ring.py` | Composants de base du DHT avec gestion des nœuds et de l’anneau (join/leave). |
| `dht_storage.py` | Étend les nœuds avec un système de stockage (clé/valeur), réplication, et transfert de données. |
| `dht_routing.py` | Implémente un routage basique entre nœuds dans l'anneau via transmission de messages. |
| `dht_advanced_routing.py` | Routage avancé avec liens longs, table des doigts, cache dynamique et protocole Chord-like. |

---

## Fonctionnalités

### DHT Anneau Dynamique
- Rejoint ou quitte dynamiquement des nœuds
- Mise à jour automatique des voisins

### Stockage Distribué
- Stockage clé/valeur local
- Réplication sur voisins gauche et droite
- Hachage SHA1 des clés pour affectation des responsabilités

### Routage
- **Basique** : routage circulaire simple (dans le sens horaire)
- **Avancé** : table des doigts, liens longs, cache de routage, piggybacking

### Résilience
- Transfert des données lors du départ d’un nœud
- Réplication automatique

---

## Lancer une simulation

### 1. Simulation basique :
```bash
python dht_routing.py

### 2. Simulation avec stockage :
```bash
python dht_storage.py

### 3. Simulation avec routage avancé  :
```bash
python dht_advanced_routing.py
