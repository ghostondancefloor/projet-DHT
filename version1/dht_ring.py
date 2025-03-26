import simpy
import random

class Node:
    """
    Classe représentant un nœud dans un réseau DHT (Distributed Hash Table).
    
    Chaque nœud connaît son voisin gauche et droit, gère les messages entrants, 
    et peut rejoindre ou quitter dynamiquement l'anneau.
    """

    def __init__(self, env, node_id, bootstrap_node=None):
        """
        Initialise un nœud DHT.

        Args:
            env (simpy.Environment): Environnement de simulation SimPy.
            node_id (int): Identifiant unique du nœud (0 à 99).
            bootstrap_node (Node, optional): Nœud existant pour rejoindre l'anneau.
        """
        self.env = env
        self.node_id = node_id
        self.left_neighbor = self
        self.right_neighbor = self
        self.messages = simpy.Store(env)

        if bootstrap_node:
            self.env.process(self.join(bootstrap_node))

    def __str__(self):
        """
        Retourne une représentation textuelle du nœud.
        """
        return f"Node({self.node_id})"

    def send_message(self, target_node, message_type, content=None):
        """
        Envoie un message à un autre nœud du réseau.

        Args:
            target_node (Node): Destinataire du message.
            message_type (str): Type du message (ex: 'JOIN_REQUEST').
            content (any, optional): Données supplémentaires.
        """
        message = {
            'type': message_type,
            'sender': self,
            'content': content
        }
        target_node.messages.put(message)

    def join(self, bootstrap_node):
        """
        Processus pour rejoindre l'anneau DHT via un nœud bootstrap.

        Args:
            bootstrap_node (Node): Nœud existant pour intégration dans l'anneau.
        """
        print(f"{self.env.now}: {self} demande à rejoindre l'anneau via {bootstrap_node}")
        self.send_message(bootstrap_node, 'JOIN_REQUEST')

        response = yield self.messages.get()
        if response['type'] == 'JOIN_REPLY':
            self.left_neighbor = response['content']['left_neighbor']
            self.right_neighbor = response['content']['right_neighbor']

            self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self)
            self.send_message(self.right_neighbor, 'UPDATE_LEFT', self)

            print(f"{self.env.now}: {self} a rejoint l'anneau entre {self.left_neighbor} et {self.right_neighbor}")

    def leave(self):
        """
        Processus de départ propre du nœud de l'anneau.
        Met à jour les voisins pour les reconnecter entre eux.
        """
        print(f"{self.env.now}: {self} quitte l'anneau")

        self.send_message(self.left_neighbor, 'UPDATE_RIGHT', self.right_neighbor)
        self.send_message(self.right_neighbor, 'UPDATE_LEFT', self.left_neighbor)

        print(f"{self.env.now}: {self} a quitté l'anneau, {self.left_neighbor} et {self.right_neighbor} sont maintenant connectés")

    def run(self):
        """
        Processus principal du nœud. Traite les messages entrants (JOIN, UPDATE...).
        """
        print(f"{self.env.now}: {self} démarre")
        while True:
            message = yield self.messages.get()
            msg_type = message['type']
            sender = message['sender']
            content = message['content']

            if msg_type == 'JOIN_REQUEST':
                # Recherche de la position correcte dans l'anneau
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

            elif msg_type == 'UPDATE_LEFT':
                self.left_neighbor = content
                print(f"{self.env.now}: {self} a mis à jour son voisin de gauche: {self.left_neighbor}")

            elif msg_type == 'UPDATE_RIGHT':
                self.right_neighbor = content
                print(f"{self.env.now}: {self} a mis à jour son voisin de droite: {self.right_neighbor}")


def run_simulation(duration=100, max_nodes=10):
    """
    Lance une simulation complète d'un réseau DHT dynamique.

    - Crée un premier nœud bootstrap
    - Ajoute périodiquement de nouveaux nœuds
    - Retire aléatoirement certains nœuds
    - Affiche l'état final de l'anneau

    Args:
        duration (int): Durée de la simulation (temps simulé en unités SimPy).
        max_nodes (int): Nombre maximal de nœuds à insérer.
    """
    env = simpy.Environment()
    first_node = Node(env, node_id=0)
    env.process(first_node.run())
    nodes = [first_node]

    def node_creator():
        next_node_id = 1
        while next_node_id < max_nodes:
            yield env.timeout(random.randint(5, 15))
            new_node = Node(env, node_id=next_node_id, bootstrap_node=nodes[-1])
            nodes.append(new_node)
            env.process(new_node.run())
            next_node_id += 1

    def node_remover():
        while True:
            yield env.timeout(random.randint(20, 30))
            if len(nodes) > 3:
                node = random.choice(nodes[1:])
                nodes.remove(node)
                node.leave()

    env.process(node_creator())
    env.process(node_remover())
    env.run(until=duration)
    
    print("\nÉtat final de l'anneau:")
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
