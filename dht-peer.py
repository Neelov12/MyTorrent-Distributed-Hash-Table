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

    # Manages listens and responses 
    def listen_loop(self):
        """Continuously listen for incoming UDP messages."""
        print(f"{peer_name} is listening on port {p_port}...")
        while True:
            try:
                data, addr = self.sock.recvfrom(1024)
                command = data.decode().split()
                if not command:
                    continue

                response = self.process_command(command)
                print(f"[RECEIVED] From {addr}: {' '.join(command)}")
                if "FAILURE" in command:
                    print("[SENT]No message sent")
                else:
                    print(f"[SENT] {response}\n")
                    self.sock.sendto(response.encode(), addr)
            except Exception as e:
                print("[ERROR in listen_loop]", e)

    def input_loop(self):
        # Prompts user to issue command to manager
        while True: 
            print(f"\nPeer {peer_name}, enter a command at any time)")
            print("1: Exit")
            print("2: Set up DHT (setup-dht)")
            print(f"3: Listen on your port {p_port}")
            option = input("\nSelect an option: ").strip()

            if option == "1": 
                sys.exit() 
                break
            elif option == "2":
                n_size = input("Select size of hash table (integer): ")
                y = input("Select year of storm data (YYYY): ")
                peer.setup_dht(n_size, y)
            elif option == "3":
                handle_setup_dht()
                break
            else:
                print("Invalid choice. Please enter a valid number.")

    def start(self):
        """Start listening and input threads."""
        threading.Thread(target=self.listen_loop, daemon=True).start()
        self.input_loop()  # Run input loop on main thread (so user can Ctrl+C)

    # Manages appropriate response to command 
    def process_command(self, command):
        cmd_type = command[0]
        resp_type = command[0]+command[1]
        
        if cmd_type == "set-id" and len(command) == 5:
            return self.handle_register(command[1], command[2], int(command[3]), int(command[4]))
        elif resp_type == "SUCCESS 2":
            self.initialize_ring(command)
            return self.handle_setup_dht(command[1], int(command[2]), command[3])
        elif cmd_type == "dht-complete" and len(command) == 2:
            return self.dht_complete(command[1])
        else:
            return "FAILURE Invalid command"

    # REGISTER (Send) 
    # (peer_name, peer ip, m_port, p_port) 
    def register(self):
        """Register with the DHT manager."""
        message = f"register {self.peer_name} {self.peer_ip} {self.m_port} {self.p_port}"
        self.sock.sendto(message.encode(), (self.manager_ip, self.manager_port))
        response, _ = self.sock.recvfrom(1024)
        print("Manager Response:", response.decode())

    # SETUP-DHT (Send)
    # (peer_name, n, yyyy)
    def setup_dht(self, n, year):
        """Send setup-dht request to the manager."""
        message = f"setup-dht {self.peer_name} {n} {year}"
        self.sock.sendto(message.encode(), (self.manager_ip, self.manager_port))

    # Initialize the ring, store all peers information 
    def initialize_ring(self, response):
        """Initialize the DHT ring structure."""
        lines = response.split('\n')[2:]
        self.peers = {i: tuple(lines[i].split()) for i in range(len(lines))}
        print("DHT Ring Established:", self.peers)
        self.set_ids()

    # Assign ids to all the peers 
    def set_ids(self):
        """Assign IDs to peers and establish the ring topology."""
        self.id = 0  # Leader ID
        n = len(self.peers)
        for i in range(1, n):
            peer_name, peer_ip, peer_port = self.peers[i]
            set_id_msg = f"set-id {i} {n} " + " ".join(
                ["{},{},{}".format(*self.peers[j]) for j in range(n)]
            )
            print(f"[SENT] {set_id_msg}")
            self.sock.sendto(set_id_msg.encode(), (peer_ip, int(peer_port)))


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

    # Prompts user to register  
    while True:
        confirmation = input("\nWould you like to register? (Y/N): ").strip().lower()
        if confirmation == "y":
            peer.register()
            print("\nContinuing...")
            break 
        else:
            print("No commands can be sent to manager before registration. Rerequesting...")

    # Starts multithreading socket and input from user 
    peer.start() 
    




        
    

