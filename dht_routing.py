import simpy
import random
from dht_ring import Node  # Importation du code du premier fichier

def run_simulation(duration=100):
    env = simpy.Environment()
    first_node = Node(env, node_id=0)
    env.process(first_node.run())
    nodes = [first_node]
    
    def node_creator():
        next_node_id = 1
        while next_node_id < 10:
            yield env.timeout(random.randint(5, 15))
            new_node = Node(env, node_id=next_node_id, bootstrap_node=nodes[-1])
            nodes.append(new_node)
            env.process(new_node.run())
            next_node_id += 1
    
    def message_sender():
        while True:
            yield env.timeout(random.randint(10, 20))
            if len(nodes) > 1:
                sender = random.choice(nodes)
                target = random.choice(nodes)
                print(f"{env.now}: {sender} veut envoyer un message à {target}")
                env.process(forward_message(sender, target, "MSG", "wsh pelo"))
    
    def forward_message(sender, target_node, message_type, content):
        """
        Envoie un message en suivant l'anneau dans le sens des aiguilles d'une montre 
        jusqu'à atteindre le nœud cible.
        """
        current_node = sender
        hops = 0
        max_hops = len(nodes) * 2  # Limite pour éviter une boucle infinie
        
        while current_node != target_node and hops < max_hops:
            next_hop = current_node.right_neighbor
            print(f"{env.now}: {current_node} transfère le message à {next_hop}")
            yield env.timeout(1)  # Simulation du délai de transmission
            current_node = next_hop
            hops += 1
        
        if current_node == target_node:
            print(f"{env.now}: {target_node} a reçu le message: {message_type} - {content}")
        else:
            print(f"{env.now}: Le message n'a pas pu atteindre {target_node}")
    
    env.process(node_creator())
    env.process(message_sender())
    env.run(until=duration)

if __name__ == "__main__":
    run_simulation(200)
