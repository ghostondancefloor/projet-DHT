import simpy
import random
from dht_ring import Node  # Importation du code du premier fichier

def run_simulation(duration=100):
    env = simpy.Environment()
    first_node = Node(env, node_id=0)
    env.process(first_node.run())
    nodes = [first_node]
    
    def node_creator():
        for _ in range(1, 10):
            yield env.timeout(random.randint(5, 15))
            new_node = Node(env, bootstrap_node=random.choice(nodes))
            nodes.append(new_node)
            env.process(new_node.run())
    
    def message_sender():
        while True:
            yield env.timeout(random.randint(10, 20))
            if len(nodes) > 1:
                sender = random.choice(nodes)
                target = random.choice(nodes)
                print(f"{env.now}: {sender} envoie un message à {target}")
                env.process(forward_message(sender, target, "TEST_MESSAGE", "Ceci est un test"))
    
    def forward_message(current_node, target_node, message_type, content):
        """Envoie un message en suivant l'anneau jusqu'à la cible."""
        while current_node != target_node:
            next_hop = current_node.right_neighbor  # Routage par la droite
            print(f"{env.now}: {current_node} transfère le message à {next_hop}")
            yield env.timeout(1)  # Simulation du délai de transmission
            current_node = next_hop
        print(f"{env.now}: {target_node} a reçu le message: {message_type} - {content}")
    
    env.process(node_creator())
    env.process(message_sender())
    env.run(until=duration)

if __name__ == "__main__":
    run_simulation(150)
