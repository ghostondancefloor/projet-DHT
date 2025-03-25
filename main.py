import argparse
import time
from dht_ring import run_simulation as run_ring_simulation
from dht_routing import run_simulation as run_routing_simulation
from dht_storage import run_storage_simulation

def print_header(title):
    """Affiche un en-tête formaté pour une meilleure lisibilité"""
    print("\n" + "=" * 80)
    print(f" {title} ".center(80, '='))
    print("=" * 80 + "\n")

def run_simple_ring_demo(duration=150):
    """Exécute la simulation de l'anneau simple (étape 1)"""
    print_header("SIMULATION DE L'ANNEAU DHT (ÉTAPE 1)")
    print("Cette simulation démontre le comportement de base de l'anneau DHT:")
    print(" - Création de nœuds qui rejoignent l'anneau")
    print(" - Suppression aléatoire de nœuds (qui quittent correctement l'anneau)")
    print(" - Vérification de l'intégrité de l'anneau à la fin\n")
    
    run_ring_simulation(duration)

def run_routing_demo(duration=150):
    """Exécute la simulation du routage de messages (étape 2)"""
    print_header("SIMULATION DU ROUTAGE DE MESSAGES (ÉTAPE 2)")
    print("Cette simulation démontre le routage de messages dans l'anneau DHT:")
    print(" - Création de l'anneau comme dans l'étape 1")
    print(" - Envoi aléatoire de messages test entre les nœuds")
    print(" - Affichage du routage des messages à travers l'anneau\n")
    
    run_routing_simulation(duration)

def run_storage_demo(duration=300, num_nodes=10, node_removal=True):
    """Exécute la simulation du stockage de données (étape 3)"""
    print_header("SIMULATION DU STOCKAGE DE DONNÉES (ÉTAPE 3)")
    print("Cette simulation démontre le stockage et la récupération de données dans la DHT:")
    print(" - Création d'un anneau avec des nœuds capables de stocker des données")
    print(f" - {num_nodes} nœuds seront créés au total")
    print(" - Opérations PUT pour stocker des données dans la DHT")
    print(" - Opérations GET pour récupérer des données depuis la DHT")
    if node_removal:
        print(" - Retrait de nœuds pour tester la résilience du système")
    print(" - Vérification des données stockées à la fin\n")
    
    run_storage_simulation(duration, num_nodes, node_removal)

def run_full_demo():
    """Exécute séquentiellement les trois démonstrations"""
    run_simple_ring_demo(100)
    time.sleep(2)  # Pause entre les démos
    run_routing_demo(100)
    time.sleep(2)  # Pause entre les démos
    run_storage_demo(200)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulation d'une DHT (Distributed Hash Table)")
    parser.add_argument("--demo", type=str, choices=["ring", "routing", "storage", "full"], 
                        default="full", help="Type de démonstration à exécuter")
    parser.add_argument("--duration", type=int, default=300, 
                        help="Durée de la simulation (par défaut: 300)")
    parser.add_argument("--nodes", type=int, default=10, 
                        help="Nombre de nœuds à créer pour la simulation de stockage (par défaut: 10)")
    parser.add_argument("--stable", action="store_true", 
                        help="Désactive la suppression aléatoire de nœuds pour un anneau stable")
    
    args = parser.parse_args()
    
    # Configuration de la simulation selon les arguments
    if args.demo == "ring":
        run_simple_ring_demo(args.duration)
    elif args.demo == "routing":
        run_routing_demo(args.duration)
    elif args.demo == "storage":
        run_storage_demo(args.duration, args.nodes, not args.stable)
    else:  # full demo
        run_full_demo()