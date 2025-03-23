import simpy
import random
import uuid
import hashlib
from dht_ring import Node as BaseNode  # Importer la classe de base de l'étape 1

class StorageNode(BaseNode):
    """Extension de la classe Node avec des capacités de stockage"""
    
    def __init__(self, env, node_id=None, bootstrap_node=None):
        # Initialiser avec la classe parente
        super().__init__(env, node_id, bootstrap_node)
        # Dictionnaire pour stocker les données (clé -> valeur)
        self.data_store = {}
        # Degré de réplication (données stockées sur N nœuds)
        self.replication_degree = 3
        
    def compute_key_id(self, key):
        """Calcule l'identifiant d'une clé dans l'espace d'identifiants des nœuds"""
        # Utiliser le hash SHA-1 pour transformer la clé en identifiant
        hash_obj = hashlib.sha1(str(key).encode())
        # Convertir le hash en entier pour utiliser le même espace d'ID que les nœuds
        return int(hash_obj.hexdigest(), 16) % (2**32)
    
    def is_responsible_for(self, key_id):
        """Détermine si ce nœud est responsable d'une clé donnée"""
        # Si nous sommes le seul nœud dans l'anneau, nous sommes responsables
        if self.right_neighbor == self:
            return True
            
        # Si nous sommes avant notre voisin de droite dans l'ordre des IDs
        if self.node_id < self.right_neighbor.node_id:
            # La clé est entre nous et notre voisin de droite
            return self.node_id <= key_id < self.right_neighbor.node_id
        else:
            # L'anneau "boucle" autour de l'espace d'identifiants
            return key_id >= self.node_id or key_id < self.right_neighbor.node_id
    
    def run(self):
        """Version étendue du processus principal pour traiter les messages de stockage"""
        print(f"{self.env.now}: {self} démarre (avec stockage)")
        while True:
            message = yield self.messages.get()
            
            msg_type = message['type']
            sender = message['sender']
            content = message['content']
            
            # Gestion des messages d'étape 1 (join/leave)
            if msg_type == 'JOIN_REQUEST':
                # (code existant pour gérer le join - inchangé)
                current = self
                next_node = self.right_neighbor
                
                if current == next_node:
                    self.right_neighbor = sender
                    self.left_neighbor = sender
                    self.send_message(sender, 'JOIN_REPLY', {
                        'left_neighbor': self, 
                        'right_neighbor': self
                    })
                    # Transférer les données dont le nouveau nœud est responsable
                    self.env.process(self.transfer_data_after_join(sender))
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
                
                # Planifier le transfert de données après le join
                if current == self:
                    self.env.process(self.transfer_data_after_join(sender))
            
            elif msg_type == 'UPDATE_LEFT':
                self.left_neighbor = content
                print(f"{self.env.now}: {self} a mis à jour son voisin de gauche: {self.left_neighbor}")
                # Vérifier si des données doivent être répliquées sur le nouveau voisin
                self.env.process(self.replicate_data_to_neighbor(self.left_neighbor, 'left'))
            
            elif msg_type == 'UPDATE_RIGHT':
                self.right_neighbor = content
                print(f"{self.env.now}: {self} a mis à jour son voisin de droite: {self.right_neighbor}")
                # Vérifier si des données doivent être répliquées sur le nouveau voisin
                self.env.process(self.replicate_data_to_neighbor(self.right_neighbor, 'right'))
            
            # Nouveaux types de messages pour l'étape 3 (stockage)
            elif msg_type == 'PUT_REQUEST':
                key = content['key']
                value = content['value']
                original_sender = content.get('original_sender', sender)
                is_replica = content.get('is_replica', False)
                replication_count = content.get('replication_count', 0)
                
                key_id = self.compute_key_id(key)
                
                if self.is_responsible_for(key_id) or is_replica:
                    # Stocker la donnée localement
                    self.data_store[key] = value
                    print(f"{self.env.now}: {self} stocke la donnée {key}={value}" + 
                          (" (réplique)" if is_replica else " (primaire)"))
                    
                    # Si c'est une demande originale (non réplique), commencer la réplication
                    if not is_replica and replication_count < self.replication_degree - 1:
                        # Répliquer vers la droite
                        self.send_message(self.right_neighbor, 'PUT_REQUEST', {
                            'key': key,
                            'value': value,
                            'original_sender': original_sender,
                            'is_replica': True,
                            'replication_count': replication_count + 1
                        })
                    
                    # Confirmer la mise en place
                    if not is_replica:
                        self.send_message(original_sender, 'PUT_RESPONSE', {
                            'key': key,
                            'success': True
                        })
                else:
                    # Transférer au voisin qui pourrait être responsable (routage)
                    next_hop = self.find_next_hop_for_key(key_id)
                    print(f"{self.env.now}: {self} transfère la demande PUT pour {key} à {next_hop}")
                    
                    self.send_message(next_hop, 'PUT_REQUEST', {
                        'key': key,
                        'value': value,
                        'original_sender': original_sender
                    })
            
            elif msg_type == 'PUT_RESPONSE':
                # Recevoir la confirmation d'un PUT
                key = content['key']
                success = content['success']
                print(f"{self.env.now}: {self} reçoit confirmation de PUT pour {key}: {'succès' if success else 'échec'}")
            
            elif msg_type == 'GET_REQUEST':
                key = content['key']
                original_sender = content.get('original_sender', sender)
                
                key_id = self.compute_key_id(key)
                
                if self.is_responsible_for(key_id) or key in self.data_store:
                    # Vérifier si nous avons la donnée
                    if key in self.data_store:
                        value = self.data_store[key]
                        print(f"{self.env.now}: {self} a trouvé la donnée {key}={value}")
                        
                        self.send_message(original_sender, 'GET_RESPONSE', {
                            'key': key,
                            'value': value,
                            'success': True
                        })
                    else:
                        # Nous sommes responsables mais n'avons pas la donnée
                        print(f"{self.env.now}: {self} est responsable de {key} mais n'a pas cette donnée")
                        self.send_message(original_sender, 'GET_RESPONSE', {
                            'key': key,
                            'success': False,
                            'error': 'Key not found'
                        })
                else:
                    # Transférer au nœud qui pourrait être responsable
                    next_hop = self.find_next_hop_for_key(key_id)
                    print(f"{self.env.now}: {self} transfère la demande GET pour {key} à {next_hop}")
                    
                    self.send_message(next_hop, 'GET_REQUEST', {
                        'key': key,
                        'original_sender': original_sender
                    })
            
            elif msg_type == 'GET_RESPONSE':
                # Recevoir la réponse d'un GET
                key = content['key']
                success = content['success']
                if success:
                    value = content['value']
                    print(f"{self.env.now}: {self} reçoit la valeur {key}={value}")
                else:
                    error = content.get('error', 'Unknown error')
                    print(f"{self.env.now}: {self} reçoit une erreur pour GET {key}: {error}")
            
            elif msg_type == 'TRANSFER_DATA_REQUEST':
                # Recevoir une demande de transfert de données après un join
                keys_to_transfer = []
                for key in list(self.data_store.keys()):
                    key_id = self.compute_key_id(key)
                    if sender.is_responsible_for(key_id):
                        keys_to_transfer.append(key)
                
                if keys_to_transfer:
                    print(f"{self.env.now}: {self} transfère {len(keys_to_transfer)} clés à {sender}")
                    for key in keys_to_transfer:
                        # Envoyer les données au nouveau nœud
                        self.send_message(sender, 'PUT_REQUEST', {
                            'key': key,
                            'value': self.data_store[key],
                            'is_replica': False,
                            'original_sender': self
                        })
                        
                        # Si nous ne sommes plus dans la zone de réplication, supprimer la donnée
                        if not self.is_in_replication_range(key_id, sender.node_id):
                            del self.data_store[key]
                            print(f"{self.env.now}: {self} supprime {key} (transféré à {sender})")
                else:
                    print(f"{self.env.now}: {self} n'a pas de données à transférer à {sender}")
    
    def find_next_hop_for_key(self, key_id):
        """Trouve le nœud suivant pour router une requête vers la clé donnée"""
        # Stratégie simple de routage: si nous ne sommes pas responsables, transférer à droite
        return self.right_neighbor
    
    def is_in_replication_range(self, key_id, responsible_node_id):
        """Vérifie si ce nœud doit conserver une réplique pour la clé donnée"""
        # Dans notre modèle simple, nous conservons des répliques sur le nœud responsable
        # et ses voisins immédiats (gauche et droite)
        if self.node_id == responsible_node_id:
            return True
        if self.left_neighbor.node_id == responsible_node_id:
            return True
        if self.right_neighbor.node_id == responsible_node_id:
            return True
        return False
    
    def transfer_data_after_join(self, new_node):
        """Transfère les données au nouveau nœud après son join"""
        # Attendre un moment pour que le nœud s'initialise complètement
        yield self.env.timeout(2)
        print(f"{self.env.now}: {self} vérifie s'il faut transférer des données à {new_node}")
        self.send_message(new_node, 'TRANSFER_DATA_REQUEST', {})
    
    def replicate_data_to_neighbor(self, neighbor, direction):
        """Réplique les données sur un nouveau voisin"""
        # Attendre un moment pour que le nœud s'initialise
        yield self.env.timeout(2)
        print(f"{self.env.now}: {self} vérifie s'il faut répliquer des données sur {neighbor} ({direction})")
        
        # Pour chaque donnée, vérifier si elle doit être répliquée
        for key, value in self.data_store.items():
            key_id = self.compute_key_id(key)
            responsible_node = self.find_responsible_node(key_id)
            
            # Si le voisin est dans la zone de réplication pour cette clé
            if neighbor.is_in_replication_range(key_id, responsible_node.node_id):
                self.send_message(neighbor, 'PUT_REQUEST', {
                    'key': key,
                    'value': value,
                    'is_replica': True,
                    'original_sender': self
                })
                print(f"{self.env.now}: {self} réplique {key}={value} sur {neighbor}")
    
    def find_responsible_node(self, key_id):
        """Trouve le nœud responsable pour une clé donnée (approche simplifiée)"""
        # Commencer par nous-mêmes
        current = self
        
        # Parcourir l'anneau jusqu'à trouver le responsable
        while not current.is_responsible_for(key_id):
            current = current.right_neighbor
            # Éviter une boucle infinie
            if current == self:
                return self
        
        return current

def put_data(env, node, key, value):
    """Process pour mettre des données dans la DHT"""
    print(f"{env.now}: Demande de PUT {key}={value} à partir de {node}")
    key_id = node.compute_key_id(key)
    
    # Trouver le nœud responsable (approche simplifiée)
    current = node
    while not current.is_responsible_for(key_id):
        next_hop = current.find_next_hop_for_key(key_id)
        print(f"{env.now}: Routage de PUT {key} de {current} vers {next_hop}")
        current = next_hop
        yield env.timeout(1)  # Simulation du délai de routage
    
    # Envoyer la demande de PUT au nœud responsable
    node.send_message(current, 'PUT_REQUEST', {
        'key': key,
        'value': value,
        'original_sender': node
    })
    print(f"{env.now}: PUT {key}={value} envoyé au nœud responsable {current}")

def get_data(env, node, key):
    """Process pour récupérer des données de la DHT"""
    print(f"{env.now}: Demande de GET {key} à partir de {node}")
    key_id = node.compute_key_id(key)
    
    # Trouver le nœud responsable (approche simplifiée)
    current = node
    while not current.is_responsible_for(key_id):
        next_hop = current.find_next_hop_for_key(key_id)
        print(f"{env.now}: Routage de GET {key} de {current} vers {next_hop}")
        current = next_hop
        yield env.timeout(1)  # Simulation du délai de routage
    
    # Envoyer la demande de GET au nœud responsable
    node.send_message(current, 'GET_REQUEST', {
        'key': key,
        'original_sender': node
    })
    print(f"{env.now}: GET {key} envoyé au nœud responsable {current}")

def run_storage_simulation(duration=200):
    """Simulation principale pour tester le stockage dans la DHT"""
    env = simpy.Environment()
    
    # Créer le premier nœud (nœud bootstrap)
    first_node = StorageNode(env, node_id=0)
    env.process(first_node.run())
    
    # Liste pour suivre tous les nœuds
    nodes = [first_node]
    
    # Processus pour ajouter des nœuds périodiquement
    def node_creator():
        for i in range(1, 10):
            yield env.timeout(random.randint(10, 20))
            new_node = StorageNode(env, bootstrap_node=random.choice(nodes))
            nodes.append(new_node)
            env.process(new_node.run())
            print(f"{env.now}: Nouveau nœud {new_node} a été créé")
    
    # Processus pour faire quitter des nœuds périodiquement
    def node_remover():
        # Attendre que plusieurs nœuds soient créés
        yield env.timeout(50)
        while True:
            yield env.timeout(random.randint(30, 50))
            if len(nodes) > 4:  # Garder au moins 4 nœuds
                node = random.choice(nodes[1:])  # Ne pas supprimer le nœud initial
                nodes.remove(node)
                node.leave()
                print(f"{env.now}: Nœud {node} a quitté l'anneau")
    
    # Processus pour tester le stockage et la récupération de données
    def data_operations():
        # Attendre la création de quelques nœuds
        yield env.timeout(30)
        
        # Générer quelques données aléatoires
        data_keys = [f"key_{i}" for i in range(1, 11)]
        data_values = [f"value_{random.randint(100, 999)}" for _ in range(10)]
        
        # Effectuer des opérations PUT
        for i in range(10):
            key = data_keys[i]
            value = data_values[i]
            node = random.choice(nodes)
            yield env.process(put_data(env, node, key, value))
            yield env.timeout(random.randint(5, 15))
        
        # Attendre un peu avant de récupérer les données
        yield env.timeout(20)
        
        # Effectuer des opérations GET
        for i in range(10):
            key = random.choice(data_keys)
            node = random.choice(nodes)
            yield env.process(get_data(env, node, key))
            yield env.timeout(random.randint(5, 15))
        
        # Tester la récupération après un changement de topologie
        yield env.timeout(50)  # Attendre que des nœuds quittent/rejoignent
        
        # Essayer de récupérer les mêmes données
        for i in range(10):
            key = random.choice(data_keys)
            node = random.choice(nodes)
            yield env.process(get_data(env, node, key))
            yield env.timeout(random.randint(5, 15))
    
    # Lancer tous les processus
    env.process(node_creator())
    env.process(node_remover())
    env.process(data_operations())
    
    # Exécuter la simulation
    env.run(until=duration)
    
    # Afficher l'état final
    print("\nÉtat final de l'anneau:")
    for node in nodes:
        print(f"Nœud: {node} - Données stockées: {node.data_store}")
    
    # Vérifier l'intégrité de l'anneau
    print("\nVérification de l'intégrité de l'anneau:")
    current = nodes[0]
    start_id = current.node_id
    print(f"Nœud: {current} - Voisins: gauche={current.left_neighbor}, droite={current.right_neighbor}")
    current = current.right_neighbor
    
    visited = 1
    while current.node_id != start_id and visited < len(nodes):
        print(f"Nœud: {current} - Voisins: gauche={current.left_neighbor}, droite={current.right_neighbor}")
        current = current.right_neighbor
        visited += 1
    
    if visited != len(nodes):
        print(f"ATTENTION: L'anneau semble incomplet! Seulement {visited} nœuds visités sur {len(nodes)}")
    else:
        print(f"Anneau intègre: {visited} nœuds visités")

if __name__ == "__main__":
    run_storage_simulation(300)