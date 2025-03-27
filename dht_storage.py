import simpy
import random
import hashlib
from dht_ring import Node 

class StorageNode(Node):
    """
    Classe représentant un nœud DHT avec capacité de stockage.

    Hérite de Node et ajoute :
    - Le stockage local de paires clé/valeur
    - La réplication des données sur les voisins
    - Le transfert des données pertinentes lors de l'arrivée ou du départ d'un nœud

    Attributs :
        data_store (dict) : Données principales stockées localement.
        replicated_data (dict) : Données répliquées reçues des voisins.
    """
    def __init__(self, env, node_id, bootstrap_node=None):
        super().__init__(env, node_id, bootstrap_node)
        self.data_store = {} 
        self.replicated_data = {} 
    
    def run(self):
        """Processus principal du nœud pour traiter les messages, étendu pour le stockage"""
        print(f"{self.env.now}: {self} démarre (avec stockage)")
        while True:
            message = yield self.messages.get()
            
            msg_type = message['type']
            sender = message['sender']
            content = message['content']
            
            # Traitement des messages de l'anneau de base
            if msg_type == 'JOIN_REQUEST':
                # Code existant de la classe Node
                current = self
                next_node = self.right_neighbor
                
                if current == next_node:
                    self.right_neighbor = sender
                    self.left_neighbor = sender
                    self.send_message(sender, 'JOIN_REPLY', {
                        'left_neighbor': self, 
                        'right_neighbor': self
                    })
                    continue
                
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
                
                # Transférer les données pertinentes au nouveau nœud
                self.env.process(self.transfer_relevant_data(sender))
            
            elif msg_type == 'UPDATE_LEFT':
                self.left_neighbor = content
                print(f"{self.env.now}: {self} a mis à jour son voisin de gauche: {self.left_neighbor}")
                
                # Répliquer les données sur le nouveau voisin
                self.env.process(self.replicate_data_to_neighbor(self.left_neighbor))
            
            elif msg_type == 'UPDATE_RIGHT':
                self.right_neighbor = content
                print(f"{self.env.now}: {self} a mis à jour son voisin de droite: {self.right_neighbor}")
                
                # Répliquer les données sur le nouveau voisin
                self.env.process(self.replicate_data_to_neighbor(self.right_neighbor))
            
            # Nouveaux types de messages pour le stockage
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
                    # Transférer la demande vers le nœud responsable
                    print(f"{self.env.now}: {self} transfère la demande PUT pour {key} vers {self.right_neighbor}")
                    self.send_message(self.right_neighbor, 'PUT_REQUEST', content)
            
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
                    # Transférer la demande vers le nœud responsable
                    print(f"{self.env.now}: {self} transfère la demande GET pour {key} vers {self.right_neighbor}")
                    self.send_message(self.right_neighbor, 'GET_REQUEST', content)
            
            elif msg_type == 'GET_RESPONSE' or msg_type == 'PUT_CONFIRM':
                # Simple affichage de la confirmation
                if msg_type == 'GET_RESPONSE':
                    key = content['key']
                    value = content['value']
                    print(f"{self.env.now}: {self} a reçu la réponse GET pour {key}: {value}")
                else:
                    print(f"{self.env.now}: {self} a reçu la confirmation PUT pour {content['key']}")
            
            elif msg_type == 'REPLICATE':
                # Stocker localement une donnée répliquée
                key = content['key']
                value = content['value']
                self.replicated_data[key] = value
                print(f"{self.env.now}: {self} a répliqué la donnée {key}:{value}")
            
            elif msg_type == 'TRANSFER_DATA':
                # Recevoir des données transférées d'un autre nœud
                for key, value in content.items():
                    self.data_store[key] = value
                print(f"{self.env.now}: {self} a reçu {len(content)} données transférées")
    
    def generate_key_hash(self, key):
        """Génère un hash pour une clé donnée"""
        hash_object = hashlib.sha1(str(key).encode())
        hash_hex = hash_object.hexdigest()
        # Convertir en entier sur la même plage que les IDs des nœuds
        hash_int = int(hash_hex, 16) % 100  # Même plage que les node_id (0-99)
        return hash_int
    
    def find_responsible_node(self, key):
        """Trouve le nœud responsable pour une clé donnée"""
        key_hash = self.generate_key_hash(key)
        
        # Parcourir l'anneau pour trouver le nœud responsable
        current = self
        while True:
            next_node = current.right_neighbor
            
            # Cas particulier: limite de l'anneau
            if current.node_id > next_node.node_id and (key_hash > current.node_id or key_hash <= next_node.node_id):
                return next_node
            
            # Cas standard: clé entre deux nœuds consécutifs
            if current.node_id < key_hash <= next_node.node_id:
                return next_node
            
            # Si la clé correspond exactement à ce nœud
            if current.node_id == key_hash:
                return current
            
            current = next_node
            
            # Si on a fait le tour complet
            if current == self:
                return self  # Ce nœud est le plus proche
    
    def store_data(self, key, value):
        """Stocke une donnée localement"""
        self.data_store[key] = value
    
    def transfer_relevant_data(self, new_node):
        """Transfère les données pertinentes à un nouveau nœud"""
        data_to_transfer = {}
        
        # Identifier les données dont le nouveau nœud est responsable
        for key, value in list(self.data_store.items()):
            responsible_node = self.find_responsible_node(key)
            if responsible_node == new_node:
                data_to_transfer[key] = value
                del self.data_store[key]  # Ne plus stocker comme données principales
                self.replicated_data[key] = value  # Mais garder comme réplique
        
        if data_to_transfer:
            print(f"{self.env.now}: {self} transfère {len(data_to_transfer)} données à {new_node}")
            self.send_message(new_node, 'TRANSFER_DATA', data_to_transfer)
            
        yield self.env.timeout(0)  # Transforme la méthode en générateur pour SimPy
    
    def replicate_data_to_neighbor(self, neighbor):
        """Réplique les données pertinentes sur un voisin"""
        for key, value in self.data_store.items():
            self.send_message(neighbor, 'REPLICATE', {'key': key, 'value': value})
        yield self.env.timeout(0)  # Transforme la méthode en générateur pour SimPy
    
    def leave(self):
        """Méthode étendue pour gérer le transfert de données lors du départ"""
        print(f"{self.env.now}: {self} quitte l'anneau et transfère ses données")
        
        # Transférer toutes les données primaires au voisin de droite
        if self.data_store:
            self.send_message(self.right_neighbor, 'TRANSFER_DATA', self.data_store)
        
        # Informer les voisins comme dans la classe de base
        self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self.right_neighbor)
        self.send_message(self.right_neighbor, 'UPDATE_LEFT', self.left_neighbor)
        
        print(f"{self.env.now}: {self} a quitté l'anneau, {self.left_neighbor} et {self.right_neighbor} sont maintenant connectés")


def put_operation(env, node, key, value):
    """Processus pour exécuter une opération PUT"""
    print(f"{env.now}: Demande PUT {key}:{value} via {node}")
    node.send_message(node, 'PUT_REQUEST', {'key': key, 'value': value})
    yield env.timeout(0)  # Transforme la fonction en générateur pour SimPy


def get_operation(env, node, key):
    """Processus pour exécuter une opération GET"""
    print(f"{env.now}: Demande GET {key} via {node}")
    node.send_message(node, 'GET_REQUEST', {'key': key})
    yield env.timeout(0)  # Transforme la fonction en générateur pour SimPy


def run_storage_simulation(duration=100, max_nodes=10):
    env = simpy.Environment()
    
    # Créer le premier nœud (nœud bootstrap)
    first_node = StorageNode(env, node_id=0)
    env.process(first_node.run())
    
    # Liste pour suivre tous les nœuds
    nodes = [first_node]
    
    # Processus pour ajouter des nœuds périodiquement
    def node_creator():
        next_node_id = 1
        while next_node_id < max_nodes:
            yield env.timeout(random.randint(5, 15))
            
            new_node = StorageNode(env, node_id=next_node_id, bootstrap_node=random.choice(nodes))
            nodes.append(new_node)
            env.process(new_node.run())
            
            next_node_id += 1
    
    # Processus pour faire quitter des nœuds périodiquement
    def node_remover():
        while True:
            yield env.timeout(random.randint(20, 30))
            if len(nodes) > 3:  # Garder au moins quelques nœuds
                node = random.choice(nodes[1:])  # Ne pas supprimer le nœud initial
                nodes.remove(node)
                node.leave()
    
    # Variable pour suivre le nombre de données créées (partagée entre les processus)
    next_data_id = 0
    
    # Processus pour effectuer des opérations PUT périodiquement
    def data_creator():
        nonlocal next_data_id
        while True:
            yield env.timeout(random.randint(2, 8))
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
        yield env.timeout(20)  # Attendre un peu que des données soient stockées
        while True:
            yield env.timeout(random.randint(5, 10))
            if nodes and next_data_id > 0:
                # Choisir un nœud aléatoire pour initier la demande
                node = random.choice(nodes)
                # Demander une clé existante avec une haute probabilité
                key = f"key-{random.randint(0, max(0, next_data_id - 1))}"
                # Lancer l'opération GET
                env.process(get_operation(env, node, key))
    
    # Lancer les processus
    env.process(node_creator())
    env.process(node_remover())
    env.process(data_creator())
    env.process(data_retriever())
    
    # Exécuter la simulation
    env.run(until=duration)
    
    # Afficher un résumé des données stockées
    print("\nRésumé des données stockées:")
    total_primary = 0
    total_replicated = 0
    for node in nodes:
        primary_count = len(node.data_store)
        replicated_count = len(node.replicated_data)
        total_primary += primary_count
        total_replicated += replicated_count
        print(f"{node} stocke {primary_count} données primaires et {replicated_count} répliques")
    
    print(f"\nTotal: {total_primary} données primaires et {total_replicated} répliques dans le système")
