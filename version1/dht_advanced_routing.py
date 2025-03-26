import simpy
import random
import hashlib
import math
from dht_storage import StorageNode 

class AdvancedNode(StorageNode):
    """
    Classe représentant un nœud avancé dans un réseau DHT.

    Hérite de StorageNode et ajoute :
    - Le routage via des liens longs (finger table / long_links)
    - Le routage distribué via "piggybacking"
    - Une table de cache dynamique pour optimiser les chemins
    - Une simulation du protocole Chord avec vue globale (optionnelle)

    Attributs :
        long_links (dict): Liens longs directs vers d'autres nœuds.
        finger_table (dict): Table de routage "Chord-like".
        routing_cache (dict): Cache de nœuds découverts via piggybacking.
        simulator_nodes (list): Liste globale des nœuds (vue globale).
    """
    
    def __init__(self, env, node_id, bootstrap_node=None, simulator_nodes=None):
        """
    Initialise un nœud avancé dans le DHT.

    Args:
        env (simpy.Environment): Environnement SimPy.
        node_id (int): Identifiant unique du nœud.
        bootstrap_node (Node): Nœud existant à qui se connecter (optionnel).
        simulator_nodes (list): Liste des nœuds connus (utilisée pour la vue globale).
    """
        super().__init__(env, node_id, bootstrap_node)
        self.long_links = {}  
        self.finger_table = {}
        
        # Pour la méthode "avec triche"
        self.simulator_nodes = simulator_nodes
        
        # Pour la méthode "sans triche"
        self.routing_cache = {}  # Cache des nœuds rencontrés
        self.last_optimization = env.now  # Heure de la dernière optimisation
    
    def run(self):
        """Processus principal du nœud avec routage avancé"""
        print(f"{self.env.now}: {self} démarre (avec routage avancé)")
        
        # Initialiser les liens longs après un certain délai
        self.env.process(self.initialize_long_links())
        
        # Processus pour mettre à jour périodiquement les liens longs
        self.env.process(self.optimize_routing())
        
        while True:
            message = yield self.messages.get()
            
            msg_type = message['type']
            sender = message['sender']
            content = message['content']
            
            # Traitement des messages de base
            if msg_type in ['JOIN_REQUEST', 'UPDATE_LEFT', 'UPDATE_RIGHT', 'PUT_REQUEST', 
                           'GET_REQUEST', 'GET_RESPONSE', 'PUT_CONFIRM', 'REPLICATE', 'TRANSFER_DATA']:
                # Récupérer les informations de routage des messages qui passent
                if isinstance(sender, AdvancedNode):
                    self.update_routing_cache(sender)
                
                # Rediriger vers le traitement de la classe parent
                yield from self.parent_process_message(message)
            
            # Nouveaux types de messages pour le routage avancé
            elif msg_type == 'ROUTING_INFO':
                # Recevoir des informations de routage d'autres nœuds
                for node_id, node in content.items():
                    if node != self and node_id not in self.routing_cache:
                        self.routing_cache[node_id] = node
            
            elif msg_type == 'LONG_LINK_REQUEST':
                # Demande de connexion de lien long
                self.send_message(sender, 'LONG_LINK_CONFIRM', {'node_id': self.node_id})
                
            elif msg_type == 'LONG_LINK_CONFIRM':
                # Confirmation de lien long, ajouter à la table de routage
                target_id = content['node_id']
                if target_id not in self.long_links:
                    self.long_links[target_id] = sender
                    print(f"{self.env.now}: {self} a établi un lien long vers {sender}")
            
            elif msg_type == 'ADVANCED_ROUTING':
                # Message avec routage avancé
                target_id = content['target_id']
                hops = content.get('hops', 0)
                
                # Limiter le nombre de sauts pour éviter les boucles infinies
                if hops > 10:  # Maximum 10 sauts
                    print(f"{self.env.now}: {self} abandonne le message après {hops} sauts (cible: {target_id})")
                    return
                
                # Incrémenter le compteur de sauts
                content['hops'] = hops + 1
                
                if target_id == self.node_id:
                    # Message arrivé à destination
                    payload = content['payload']
                    print(f"{self.env.now}: {self} a reçu un message avancé: {payload} après {hops} sauts")
                else:
                    # Trouver le meilleur prochain saut
                    next_hop = self.find_best_next_hop(target_id)
                    
                    # Éviter de renvoyer au nœud précédent
                    if next_hop == sender:
                        # Chercher une alternative
                        alternatives = []
                        if self.right_neighbor != sender:
                            alternatives.append(self.right_neighbor)
                        if self.left_neighbor != sender:
                            alternatives.append(self.left_neighbor)
                        for node in list(self.long_links.values()) + list(self.finger_table.values()):
                            if node != sender:
                                alternatives.append(node)
                        
                        if alternatives:
                            next_hop = random.choice(alternatives)
                        else:
                            # Aucune alternative, abandonner le message
                            print(f"{self.env.now}: {self} abandonne le message (pas d'alternative à {sender})")
                            return
                    
                    print(f"{self.env.now}: {self} transfère le message avancé vers {next_hop}")
                    self.send_message(next_hop, 'ADVANCED_ROUTING', content)
    
    def parent_process_message(self, message):
        """Délégation du traitement des messages à la classe parent"""
        # Cette méthode permet d'appeler le traitement des messages de la classe parent
        # tout en conservant le comportement de générateur
        msg_type = message['type']
        sender = message['sender']
        content = message['content']
        
        if msg_type == 'JOIN_REQUEST':
            current = self
            next_node = self.right_neighbor
            
            if current == next_node:
                self.right_neighbor = sender
                self.left_neighbor = sender
                self.send_message(sender, 'JOIN_REPLY', {
                    'left_neighbor': self, 
                    'right_neighbor': self
                })
                # Partager nos informations de routage avec le nouveau nœud
                if self.routing_cache:
                    self.send_message(sender, 'ROUTING_INFO', self.routing_cache)
                yield self.env.timeout(0)
                return
            
            while True:
                if (current.node_id > next_node.node_id and 
                    (sender.node_id > current.node_id or sender.node_id < next_node.node_id)):
                    break
                if current.node_id < sender.node_id < next_node.node_id:
                    break
                current = next_node
                next_node = current.right_neighbor
                if current == self:
                    break
            
            self.send_message(sender, 'JOIN_REPLY', {
                'left_neighbor': current, 
                'right_neighbor': next_node
            })
            
            # Partager nos informations de routage avec le nouveau nœud
            if self.routing_cache:
                self.send_message(sender, 'ROUTING_INFO', self.routing_cache)
            
            # Transférer les données pertinentes au nouveau nœud
            yield from self.transfer_relevant_data(sender)
        
        elif msg_type == 'UPDATE_LEFT':
            self.left_neighbor = content
            print(f"{self.env.now}: {self} a mis à jour son voisin de gauche: {self.left_neighbor}")
            
            # Répliquer les données sur le nouveau voisin
            yield from self.replicate_data_to_neighbor(self.left_neighbor)
        
        elif msg_type == 'UPDATE_RIGHT':
            self.right_neighbor = content
            print(f"{self.env.now}: {self} a mis à jour son voisin de droite: {self.right_neighbor}")
            
            # Répliquer les données sur le nouveau voisin
            yield from self.replicate_data_to_neighbor(self.right_neighbor)
        
        elif msg_type == 'PUT_REQUEST':
            key = content['key']
            value = content['value']
            target_node = self.find_responsible_node(key)
            
            if target_node == self:
                # Ce nœud est responsable du stockage
                self.store_data(key, value)
                print(f"{self.env.now}: {self} stocke la donnée {key}:{value}")
                
                # Répliquer sur les voisins
                self.send_message(self.left_neighbor, 'REPLICATE', {'key': key, 'value': value})
                self.send_message(self.right_neighbor, 'REPLICATE', {'key': key, 'value': value})
                
                # Confirmer au demandeur
                self.send_message(sender, 'PUT_CONFIRM', {'key': key})
            else:
                # Utiliser le routage avancé au lieu du routage de proche en proche
                next_hop = self.find_best_next_hop(target_node.node_id)
                print(f"{self.env.now}: {self} transfère la demande PUT pour {key} vers {next_hop}")
                self.send_message(next_hop, 'PUT_REQUEST', content)
        
        elif msg_type == 'GET_REQUEST':
            key = content['key']
            target_node = self.find_responsible_node(key)
            
            if target_node == self:
                # Ce nœud est responsable de la donnée
                if key in self.data_store:
                    value = self.data_store[key]
                    print(f"{self.env.now}: {self} fournit la donnée {key}:{value}")
                    self.send_message(sender, 'GET_RESPONSE', {'key': key, 'value': value})
                else:
                    print(f"{self.env.now}: {self} n'a pas trouvé la donnée {key}")
                    self.send_message(sender, 'GET_RESPONSE', {'key': key, 'value': None})
            else:
                # Utiliser le routage avancé
                next_hop = self.find_best_next_hop(target_node.node_id)
                print(f"{self.env.now}: {self} transfère la demande GET pour {key} vers {next_hop}")
                self.send_message(next_hop, 'GET_REQUEST', content)
        
        elif msg_type in ['GET_RESPONSE', 'PUT_CONFIRM', 'REPLICATE', 'TRANSFER_DATA']:
            # Traiter comme dans la classe parent
            if msg_type == 'GET_RESPONSE':
                key = content['key']
                value = content['value']
                print(f"{self.env.now}: {self} a reçu la réponse GET pour {key}: {value}")
            elif msg_type == 'PUT_CONFIRM':
                print(f"{self.env.now}: {self} a reçu la confirmation PUT pour {content['key']}")
            elif msg_type == 'REPLICATE':
                key = content['key']
                value = content['value']
                self.replicated_data[key] = value
                print(f"{self.env.now}: {self} a répliqué la donnée {key}:{value}")
            elif msg_type == 'TRANSFER_DATA':
                for key, value in content.items():
                    self.data_store[key] = value
                print(f"{self.env.now}: {self} a reçu {len(content)} données transférées")
        
        yield self.env.timeout(0)
    
    # MÉTHODE 1: Avec "triche" (vue globale)
    def initialize_long_links_with_global_view(self):
        """Initialise les liens longs en utilisant la vue globale (triche)"""
        yield self.env.timeout(10)  # Attendre que le réseau se stabilise
        
        if not self.simulator_nodes:
            print(f"{self.env.now}: {self} ne peut pas initialiser les liens longs (pas de vue globale)")
            return
        
        # Méthode Chord-like: créer des liens vers des nœuds à distance 2^i
        for i in range(1, int(math.log2(100)) + 1):  # log2(100) liens longs (pour ID entre 0-99)
            # Calculer la position cible dans l'anneau
            target_id = (self.node_id + 2**i) % 100
            
            # Trouver le nœud le plus proche de cette cible
            closest_node = None
            min_distance = float('inf')
            
            for node in self.simulator_nodes:
                # Distance circulaire
                if node.node_id >= target_id:
                    distance = node.node_id - target_id
                else:
                    distance = 100 + node.node_id - target_id
                
                if distance < min_distance:
                    min_distance = distance
                    closest_node = node
            
            if closest_node and closest_node != self:
                self.finger_table[i] = closest_node
                print(f"{self.env.now}: {self} a créé un lien long vers {closest_node} (distance 2^{i})")
        
        yield self.env.timeout(0)
    
    # MÉTHODE 2: Sans "triche" (piggybacking)
    def initialize_long_links(self):
        """Initialise les liens longs de manière distribuée"""
        yield self.env.timeout(20)  # Attendre que le réseau se stabilise
        
        # Ajouter les voisins au cache de routage
        self.update_routing_cache(self.left_neighbor)
        self.update_routing_cache(self.right_neighbor)
        
        # Créer quelques liens longs aléatoires pour commencer
        if self.routing_cache:
            # Sélectionner quelques nœuds au hasard du cache
            potential_links = list(self.routing_cache.values())
            if len(potential_links) > 2:  # S'assurer qu'il y a des nœuds disponibles
                num_links = min(3, len(potential_links))
                for node in random.sample(potential_links, num_links):
                    if node != self and node != self.left_neighbor and node != self.right_neighbor:
                        self.send_message(node, 'LONG_LINK_REQUEST')
        
        yield self.env.timeout(0)
    
    def update_routing_cache(self, node):
        """Met à jour le cache de routage avec un nouveau nœud"""
        if node != self and isinstance(node, AdvancedNode):
            self.routing_cache[node.node_id] = node
    
    def optimize_routing(self):
        """Optimise périodiquement la table de routage"""
        while True:
            yield self.env.timeout(50)  # Optimiser toutes les 50 unités de temps
            
            # Partager notre cache de routage avec nos voisins
            if self.routing_cache:
                self.send_message(self.left_neighbor, 'ROUTING_INFO', self.routing_cache)
                self.send_message(self.right_neighbor, 'ROUTING_INFO', self.routing_cache)
                
                # Essayer d'établir de nouveaux liens longs basés sur la distance
                if len(self.long_links) < 5:  # Limiter le nombre de liens longs
                    # Calculer des distances cibles (comme dans Chord)
                    for i in range(1, 5):
                        target_id = (self.node_id + 2**i) % 100
                        
                        # Trouver le nœud le plus proche dans notre cache
                        closest_node = None
                        min_distance = float('inf')
                        
                        for node_id, node in self.routing_cache.items():
                            # Distance circulaire
                            if node_id >= target_id:
                                distance = node_id - target_id
                            else:
                                distance = 100 + node_id - target_id
                            
                            if distance < min_distance:
                                min_distance = distance
                                closest_node = node
                        
                        # Demander un lien long si on a trouvé un nœud approprié
                        if closest_node and closest_node != self and closest_node.node_id not in self.long_links:
                            self.send_message(closest_node, 'LONG_LINK_REQUEST')
    
    def find_best_next_hop(self, target_id):
        """Trouve le meilleur prochain saut pour atteindre une cible donnée"""
        if target_id == self.node_id:
            return self
            
        # 1. Vérifier si la cible est dans nos liens longs ou finger table
        if target_id in self.long_links:
            return self.long_links[target_id]
        
        # 2. Utiliser la finger table si elle existe (méthode avec triche)
        if self.finger_table:
            # Trouver le saut qui nous rapproche le plus
            best_distance = self.circular_distance(self.node_id, target_id)
            best_node = None
            
            for _, node in self.finger_table.items():
                distance = self.circular_distance(node.node_id, target_id)
                if distance < best_distance:
                    best_distance = distance
                    best_node = node
            
            if best_node:
                return best_node
        
        # 3. Rechercher dans le cache de routage
        if self.routing_cache:
            best_distance = self.circular_distance(self.node_id, target_id)
            best_node = None
            
            for node_id, node in self.routing_cache.items():
                distance = self.circular_distance(node_id, target_id)
                if distance < best_distance:
                    best_distance = distance
                    best_node = node
            
            if best_node:
                return best_node
        
        # 4. Utiliser le routage de base (voisin le plus proche)
        left_distance = self.circular_distance(self.left_neighbor.node_id, target_id)
        right_distance = self.circular_distance(self.right_neighbor.node_id, target_id)
        
        if left_distance < right_distance:
            return self.left_neighbor
        else:
            return self.right_neighbor
    
    def circular_distance(self, from_id, to_id):
        """Calcule la distance circulaire entre deux IDs dans l'anneau"""
        forward_distance = (to_id - from_id) % 100
        backward_distance = (from_id - to_id) % 100
        return min(forward_distance, backward_distance)


def send_message_with_advanced_routing(env, sender, target_id, message):
    """Processus pour envoyer un message avec routage avancé"""
    print(f"{env.now}: Envoi d'un message avancé de {sender} vers le nœud d'ID {target_id}")
    next_hop = sender.find_best_next_hop(target_id)
    sender.send_message(next_hop, 'ADVANCED_ROUTING', {
        'target_id': target_id,
        'payload': message,
        'hops': 0,  # Pour suivre le nombre de sauts
        'visited': [sender.node_id]  # Pour éviter les cycles
    })
    yield env.timeout(0)


def run_simulation(duration=200, max_nodes=10, use_global_view=True):
    """Exécute une simulation avec des nœuds à routage avancé"""
    env = simpy.Environment()
    
    nodes = []  # Liste pour suivre tous les nœuds (pour la vue globale)
    
    # Créer le premier nœud (nœud bootstrap)
    first_node = AdvancedNode(env, node_id=0, simulator_nodes=nodes if use_global_view else None)
    nodes.append(first_node)
    env.process(first_node.run())
    
    # Variable pour suivre le nombre de données créées
    next_data_id = 0
    
    # Processus pour ajouter des nœuds périodiquement
    def node_creator():
        nonlocal next_data_id
        next_node_id = 1
        while next_node_id < max_nodes:
            yield env.timeout(random.randint(5, 10))
            
            new_node = AdvancedNode(env, node_id=next_node_id, 
                                    bootstrap_node=random.choice(nodes),
                                    simulator_nodes=nodes if use_global_view else None)
            nodes.append(new_node)
            env.process(new_node.run())
            
            # Si on utilise la "triche", initialiser les liens longs
            if use_global_view:
                env.process(new_node.initialize_long_links_with_global_view())
            
            next_node_id += 1
    
    # Processus pour faire quitter des nœuds périodiquement (moins fréquent)
    def node_remover():
        while True:
            yield env.timeout(random.randint(40, 60))  # Moins fréquent
            if len(nodes) > 3:  # Garder au moins quelques nœuds
                node = random.choice(nodes[1:])  # Ne pas supprimer le nœud initial
                nodes.remove(node)
                node.leave()
    
    # Processus pour effectuer des opérations PUT périodiquement
    def data_creator():
        nonlocal next_data_id
        while True:
            yield env.timeout(random.randint(4, 12))  # Moins fréquent
            if nodes:
                # Choisir un nœud aléatoire pour initier la demande
                node = random.choice(nodes)
                # Créer une clé et une valeur
                key = f"key-{next_data_id}"
                value = f"value-{next_data_id}"
                # Lancer l'opération PUT
                env.process(put_operation(env, node, key, value))
                next_data_id += 1
    
    # Processus pour effectuer des opérations GET périodiquement
    def data_retriever():
        nonlocal next_data_id
        yield env.timeout(30)  # Attendre un peu que des données soient stockées
        while True:
            yield env.timeout(random.randint(8, 15))  # Moins fréquent
            if nodes and next_data_id > 0:
                # Choisir un nœud aléatoire pour initier la demande
                node = random.choice(nodes)
                # Demander une clé existante avec une haute probabilité
                key = f"key-{random.randint(0, max(0, next_data_id - 1))}"
                # Lancer l'opération GET
                env.process(get_operation(env, node, key))
    
    # Processus pour tester le routage avancé
    def advanced_routing_tester():
        yield env.timeout(60)  # Attendre que le réseau se stabilise
        for _ in range(5):  # Limiter à 5 tests seulement
            yield env.timeout(15)
            if len(nodes) > 3:
                sender = random.choice(nodes)
                # Choisir un ID de nœud cible aléatoire (pas nécessairement existant)
                target_id = random.randint(0, 99)
                message = f"Message-{random.randint(1000, 9999)}"
                
                # Envoyer un message avec routage avancé
                env.process(send_message_with_advanced_routing(env, sender, target_id, message))
    
    # Lancer les processus
    env.process(node_creator())
    env.process(node_remover())
    env.process(data_creator())
    env.process(data_retriever())
    env.process(advanced_routing_tester())
    
    # Exécuter la simulation
    env.run(until=duration)
    
    # Analyser les tables de routage
    print("\nAnalyse des tables de routage:")
    total_normal_links = 0
    total_long_links = 0
    
    for node in nodes:
        num_long_links = len(node.long_links) + len(node.finger_table)
        total_normal_links += 2  # Liens gauche et droite
        total_long_links += num_long_links
        print(f"{node} a {num_long_links} liens longs et 2 liens normaux")
    
    if nodes:
        print(f"\nMoyenne de liens longs par nœud: {total_long_links / len(nodes):.2f}")
    print(f"Total: {total_normal_links} liens normaux et {total_long_links} liens longs dans le système")


def put_operation(env, node, key, value):
    """Processus pour exécuter une opération PUT"""
    print(f"{env.now}: Demande PUT {key}:{value} via {node}")
    node.send_message(node, 'PUT_REQUEST', {'key': key, 'value': value})
    yield env.timeout(0)


def get_operation(env, node, key):
    """Processus pour exécuter une opération GET"""
    print(f"{env.now}: Demande GET {key} via {node}")
    node.send_message(node, 'GET_REQUEST', {'key': key})
    yield env.timeout(0)


if __name__ == "__main__":
    # Par défaut, utilise la vue globale (triche)
    use_global_view = True
    
    # Pour tester sans la vue globale, changer à False
    # use_global_view = False
    
    run_simulation(500, max_nodes=15, use_global_view=use_global_view)