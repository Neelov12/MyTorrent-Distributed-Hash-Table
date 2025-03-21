import socket
import sys
import random
import threading
import csv
import hashlib

# Define the allowed port range based on G = 34
# For group 34 it is 18000-18499
G = 34
PORT_RANGE = range(((G // 2) * 1000) + 1000, ((G // 2) * 1000) + 1499)

class DHTPeer: 
    def __init__(self, manager_ip, manager_port, peer_name, peer_ip, m_port, p_port):

        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.peer_name = peer_name
        self.peer_ip = peer_ip
        self.m_port = m_port
        self.p_port = p_port
        self.peers = {}  # Stores peer info in the ring
        self.local_dht = {}  # Local hash table for storm data
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.peer_ip, self.m_port))
        self.id = None
        self.ring_size = None
        self.right_neighbor = None

    # REGISTER (Send) 
    # (peer_name, peer ip, m_port, p_port) 
    def register(self):
        """Register with the DHT manager."""
        message = f"register {self.peer_name} {self.peer_ip} {self.m_port} {self.p_port}"
        self.sock.sendto(message.encode(), (self.manager_ip, self.manager_port))
        response, _ = self.sock.recvfrom(1024)
        print("Manager Response:", response.decode())


if __name__ == "__main__":
    # port = int(input("Enter manager port number: (Range is 18000-18499)"))
    # Throws error if 2 arguments are not passed 
    if len(sys.argv) != 3:
        print("Usage: python dht-peer.py <manager_ip> <manager_port (18000-18499)>")
        sys.exit(1)

    # Sets manager ip and port as arguments passed in command line, peer ip auto assigned 
    manager_ip = sys.argv[1]
    manager_port = int(sys.argv[2])
    peer_ip = socket.gethostbyname(socket.gethostname())

    print("BECOME A PEER")
    configuring = True 
    while configuring:
        # Takes peer name as input, throws error if 15 > characters
        peer_name = input("Enter peer name (max 15 characters): ").strip()
        while len(peer_name) > 15 or not peer_name.isalpha():
            print("Peer name must be alphabetic and at most 15 characters.")
            peer_name = input("Enter peer name: ").strip()

        # Takes manager and peer port and requests again if either is not in range
        m_port = int(input("Enter manager port (m_port): ").strip())
        p_port = int(input("Enter peer-to-peer port (p_port): ").strip())
        while m_port not in PORT_RANGE or p_port not in PORT_RANGE:
            print("Ports must be integers and must be in the range 18000 - 18499.\n")
            m_port = int(input("Enter manager port (m_port): ").strip())
            p_port = int(input("Enter peer-to-peer port (p_port): ").strip())
        
        # List configuration to user and request confirmation to continue
        print("\nPeer Configuration:")
        print(f"Name: {peer_name}")
        print(f"Manager Port (m_port): {m_port}")
        print(f"Peer Port (p_port): {p_port}")
        print(f"Peer IP Address (automatically configured): {peer_ip}") 
        confirmation = input("\nContinue with this configuration? (Y/N): ").strip().lower()
        if confirmation == "y":
            configuring = False
            print("\nContinuing...")

    # Initialize peer object 
    peer = DHTPeer(manager_ip, manager_port, peer_name, peer_ip, m_port, p_port)

    # Prompts user for command 
    while True: 
        print(f"\nPeer {peer_name}, what would you like to do?")
        print("1: Register (register)")
        print("2: Set up DHT (setup-dht)")
        print(f"3: Listen on your port {p_port}")
        option = input("\nSelect an option: ").strip()

        if option == "1":
            peer.register()
            break
        elif option == "2":
            handle_setup_dht()
            break
        else:
            print("Invalid choice. Please enter a valid number.")

        
    

