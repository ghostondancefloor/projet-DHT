import simpy
import random

class Node:
    def __init__(self, env, node_id, bootstrap_node=None):
        self.env = env
        self.node_id = node_id
        self.left_neighbor = self
        self.right_neighbor = self
        self.messages = simpy.Store(env)
        
        # Si un nœud bootstrap est fourni, rejoindre le réseau
        if bootstrap_node:
            self.env.process(self.join(bootstrap_node))
        
    def __str__(self):
        return f"Node({self.node_id})"
    
    def send_message(self, target_node, message_type, content=None):
        """Envoie un message à un autre nœud"""
        message = {
            'type': message_type,
            'sender': self,
            'content': content
        }
        target_node.messages.put(message)
    
    def join(self, bootstrap_node):
        """Processus pour rejoindre l'anneau via un nœud existant"""
        print(f"{self.env.now}: {self} demande à rejoindre l'anneau via {bootstrap_node}")
        self.send_message(bootstrap_node, 'JOIN_REQUEST')
        
        # Attendre la réponse pour connaître sa position
        response = yield self.messages.get()
        if response['type'] == 'JOIN_REPLY':
            self.left_neighbor = response['content']['left_neighbor']
            self.right_neighbor = response['content']['right_neighbor']
            
            # Notifier les voisins
            self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self)
            self.send_message(self.right_neighbor, 'UPDATE_LEFT', self)
            
            print(f"{self.env.now}: {self} a rejoint l'anneau entre {self.left_neighbor} et {self.right_neighbor}")
    
    def leave(self):
        """Processus pour quitter l'anneau proprement"""
        print(f"{self.env.now}: {self} quitte l'anneau")
        
        # Informer les voisins
        self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self.right_neighbor)
        self.send_message(self.right_neighbor, 'UPDATE_LEFT', self.left_neighbor)
        
        print(f"{self.env.now}: {self} a quitté l'anneau, {self.left_neighbor} et {self.right_neighbor} sont maintenant connectés")
    
    def run(self):
        """Processus principal du nœud pour traiter les messages"""
        print(f"{self.env.now}: {self} démarre")
        while True:
            message = yield self.messages.get()
            
            msg_type = message['type']
            sender = message['sender']
            content = message['content']
            
            if msg_type == 'JOIN_REQUEST':
                # Gestion directe ici au lieu d'appeler une autre méthode avec process
                # Trouver la position correcte dans l'anneau
                current = self
                next_node = self.right_neighbor
                
                # Si nous sommes le seul nœud dans l'anneau
                if current == next_node:
                    self.right_neighbor = sender
                    self.left_neighbor = sender
                    self.send_message(sender, 'JOIN_REPLY', {
                        'left_neighbor': self, 
                        'right_neighbor': self
                    })
                    continue
                
                # Parcourir l'anneau pour trouver la position
                while True:
                    # Cas particulier: ID au début/fin de l'anneau
                    if (current.node_id > next_node.node_id and 
                        (sender.node_id > current.node_id or sender.node_id < next_node.node_id)):
                        break
                        
                    # Cas standard: ID entre deux nœuds consécutifs
                    if current.node_id < sender.node_id < next_node.node_id:
                        break
                    
                    # Passer au nœud suivant
                    current = next_node
                    next_node = current.right_neighbor
                    
                    # Si on a fait le tour complet de l'anneau
                    if current == self:
                        break
                
                # Envoyer la réponse avec les nouveaux voisins
                self.send_message(sender, 'JOIN_REPLY', {
                    'left_neighbor': current, 
                    'right_neighbor': next_node
                })
            
            elif msg_type == 'UPDATE_LEFT':
                self.left_neighbor = content
                print(f"{self.env.now}: {self} a mis à jour son voisin de gauche: {self.left_neighbor}")
            
            elif msg_type == 'UPDATE_RIGHT':
                self.right_neighbor = content
                print(f"{self.env.now}: {self} a mis à jour son voisin de droite: {self.right_neighbor}")


# Simulation
def run_simulation(duration=100, max_nodes=10):
    env = simpy.Environment()
    
    # Créer le premier nœud (nœud bootstrap)
    first_node = Node(env, node_id=0)
    env.process(first_node.run())
    
    # Liste pour suivre tous les nœuds
    nodes = [first_node]
    
    # Processus pour ajouter des nœuds périodiquement avec des IDs séquentiels
    def node_creator():
        next_node_id = 1
        while next_node_id < max_nodes:
            yield env.timeout(random.randint(5, 15))
            
            # Créer un nouveau nœud avec l'ID séquentiel suivant
            new_node = Node(env, node_id=next_node_id, bootstrap_node=nodes[-1])
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
    
    env.process(node_creator())
    env.process(node_remover())
    
    # Exécuter la simulation
    env.run(until=duration)
    
    # Afficher l'état final de l'anneau
    print("\nÉtat final de l'anneau:")
    current = nodes[0]
    start_id = current.node_id
    print(f"Nœud: {current} - Voisins: gauche={current.left_neighbor}, droite={current.right_neighbor}")
    current = current.right_neighbor
    
    # Vérifier si nous avons un anneau valide
    visited = 1
    while current.node_id != start_id and visited < len(nodes):
        print(f"Nœud: {current} - Voisins: gauche={current.left_neighbor}, droite={current.right_neighbor}")
        current = current.right_neighbor
        visited += 1
        
    # Vérifier l'intégrité de l'anneau
    if visited != len(nodes):
        print(f"ATTENTION: L'anneau semble incomplet! Seulement {visited} nœuds visités sur {len(nodes)}")

if __name__ == "__main__":
    run_simulation(2000, max_nodes=100)