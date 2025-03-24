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
        self.id = None
        self.ring_size = None
        self.right_neighbor = None
        self.CSV_FILE = "details-1950.csv"
        self.REQUIRED_FIELDS = [
            "EVENT_ID", "STATE", "YEAR", "MONTH_NAME", "EVENT_TYPE", "CZ_TYPE", "CZ_NAME",
            "INJURIES_DIRECT", "INJURIES_INDIRECT", "DEATHS_DIRECT", "DEATHS_INDIRECT",
            "DAMAGE_PROPERTY", "DAMAGE_CROPS", "TOR_F_SCALE"
        ]

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", p_port))


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

                print(f"[RECEIVED] From {addr}: {' '.join(command)}")

                response = self.process_command(command)
                if command[0] == "FAILURE" or response == "Disregard":
                    print("[SENT] No response sent\n")
                else:
                    print(f"[SENT] To {addr}: {response}\n")
                    self.sock.sendto(response.encode(), addr)
            except Exception as e:
                print("[ERROR in listen_loop]", e)

    def input_loop(self):
        # Prompts user to issue command to manager
        while True: 
            print(f"\nPeer {peer_name}, enter a command at any time)")
            print("1: Exit")
            print("2: Set up DHT (setup-dht)")
            option = input("\nSelect an option: \n").strip()

            if option == "1": 
                sys.exit() 
                break
            elif option == "2":
                n_size = input("Select size of hash table (integer): ")
                y = input("Select year of storm data (YYYY): ")
                peer.setup_dht(n_size, y)
            else:
                print("Invalid choice. Please enter a valid number.")

    def start(self):
        """Start listening and input threads."""
        threading.Thread(target=self.listen_loop, daemon=True).start()
        self.input_loop()  # Run input loop on main thread (so user can Ctrl+C)

    # Manages appropriate response to command 
    def process_command(self, command):
        cmd_type = command[0]
        resp_type = command[0]+" "+command[1]
        
        # If received command is set-id
        if cmd_type == "set-id":
            return self.handle_set_id(command)
        # If received command is SUCCESS from manager after sending set-dht 
        elif resp_type == "SUCCESS 2":
            return self.initialize_ring(' '.join(command)) # Returns dht-complete <self.peer_name> if no issues
        elif cmd_type == "dht-complete" and len(command) == 2:
            return self.dht_complete(command[1])
        # If received command is SUCCESS from peers after sending set-id (only sent by leader)
        elif resp_type == "SUCCESS 3":
            return "Disregard" # Prompts listen_loop to send nothing 
        # If received command is store from leader or peer 
        elif cmd_type == "store":
            return self.handle_store(command) # Returns "Disregard" - so no response is sent
        # Response from manager if dht-complete is successful 
        elif resp_type == "SUCCESS 4":
            print("Manager confirms DHT is successfully set up")
            return "Disregard"
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
        lines = response.split(' ')[2:]
        self.peers = {
            i: tuple(
            val for j, val in enumerate(lines[i].strip('()').split(',')) if j != 2)
            for i in range(len(lines))
        }
        print("[INFO] DHT Ring Established:", self.peers)
        return self.set_ids()

    # Assign ids to all the peers 
    def set_ids(self):
        """Assign IDs to peers and establish the ring topology."""
        # First, set own id, ring size, and right neighbor 
        self.id = 0  # Leader ID
        n = len(self.peers)
        self.ring_size = n
        next_id = self.id+1 % self.ring_size
        self.right_neighbor = self.peers[self.id+1 % self.ring_size]
        # Output info message to terminal 
        print(f"[INFO] Assigned ID: {self.id}")
        print(f"[INFO] Ring size: {self.ring_size}")
        print(f"[INFO] Right neighbor (ID {next_id}): {self.right_neighbor}")
        # Next, set the ids to the other peers
        #   and send each their id and all peer information 
        for i in range(1, n):
            peer_name, peer_ip, peer_port = self.peers[i]
            set_id_msg = f"set-id {i} {n} " + " ".join(
                ["{},{},{},{}".format(j, *self.peers[j]) for j in range(n)]
            )
            print(f"[SENT] To {peer_name} on {peer_port}: {set_id_msg}")
            self.sock.sendto(set_id_msg.encode(), (peer_ip, int(peer_port)))
        return self.construct_dht()
    
    def construct_dht(self): 
        records = self.load_filtered_data(self.CSV_FILE)
        num_events = len(records)  # l
        print(f"\n[INFO]Number of events (l): {num_events}")

        #  s - hash table size 
        s = self.next_prime(2 * num_events)
        print(f"[INFO]Next prime greater than 2*l: {s}")

        # Tracks stored records in each peer
        stored_rec_size = self.ring_size 
        stored_recs = [0 for _ in range(stored_rec_size)]
        # Calculate id and pos for each record using EVENT_ID
        for rec in records:
            try:
                event_id = int(rec[0])
                pos = event_id % s
                peer_id = pos % self.ring_size
                if peer_id == self.id:
                    stored_recs[self.id] += 1
                    self.local_dht[pos] = rec
                else:
                    stored_recs[peer_id] += 1
                    self.store(peer_id, pos, rec)  
            except ValueError:
                print(f"[WARN] Invalid EVENT_ID: {rec[0]}")

        print("\n[SUMMARY] Records sent to each peer:")
        for peer_id in range(self.ring_size):
            count = stored_recs[peer_id]
            peer_name, peer_ip, peer_port = self.peers[peer_id]
            print(f"Peer ID {peer_id} ({peer_name} at {peer_ip}:{peer_port}) received {count} records.")

        return self.dht_complete()

    # Helper function for construct_dht
    def is_prime(self, n):
        if n <= 1: return False
        if n == 2: return True
        if n % 2 == 0: return False
        for i in range(3, int(n**0.5)+1, 2):
            if n % i == 0:
                return False
        return True
    
    # Helper function for construct_dht
    def next_prime(self, start):
        while not self.is_prime(start):
            start += 1
        return start
    
    # Helper function for construct_dht, filters csv to only include tuples for project 
    def load_filtered_data(self, filepath):
        filtered_records = []
        with open(filepath, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                filtered_tuple = tuple(row[field] for field in self.REQUIRED_FIELDS)
                filtered_records.append(filtered_tuple)
        return filtered_records
    
    # Send store command 
    def store(self, peer_id, pos, rec):
        """Handle incoming set-id command from leader."""
        if peer_id not in self.peers:
            print(f"[WARN] Peer ID {peer_id} not found in ring.")
            return
        
        #peer_name, peer_ip, peer_port = self.peers[peer_id]
        # Find neighbor name, ip, and port 
        n_name, n_ip, n_port = self.right_neighbor 
        rec_data = " ".join(rec)
        store_msg = f"store {peer_id} {pos} {rec_data}"
        print(f"[SENT] To {n_name} at {n_port}: {store_msg}")
        self.sock.sendto(store_msg.encode(), (n_ip, int(n_port)))

    def dht_complete(self):
        return f"dht-complete {self.peer_name}"


    # Handles set-id commands from peer leader
    def handle_set_id(self, command):
        # Sets id and ring size to id and ring size sent by leader
        self.id = int(command[1])
        self.ring_size = int(command[2])

        # Parse the peer list that follows
        peer_tuples = command[3:]

        # Reconstruct self.peers as a dictionary
        self.peers = {}
        for entry in peer_tuples:
            parts = entry.split(",")
            peer_id = int(parts[0])
            self.peers[peer_id] = tuple(parts[1:])  # ('peer_name', 'ip', 'port')

        # Set right neighbor
        next_id = (self.id + 1) % self.ring_size
        self.right_neighbor = self.peers[next_id]

        print(f"[INFO] Assigned ID: {self.id}")
        print(f"[INFO] Ring size: {self.ring_size}")
        print(f"[INFO] Right neighbor (ID {next_id}): {self.right_neighbor}")

        # Uncomment to debug - Sends success message to leader, proper application encoding not needed since leader disregards this message 
        #return f"SUCCESS 3 Assigned-ID:{self.id} Ring size:{self.ring_size} Right neighbor (ID {next_id}): {self.right_neighbor}"
        return "Disregard"

    # Handles store commands 
    def handle_store(self, command):
        # store command structure = store <peer_id> <pos> <rec> 
        peer_id = int(command[1])        
        pos = int(command[2])             
        rec = tuple(command[3:])

        # If store is intended for this peer 
        if self.id == peer_id:
            self.local_dht[pos] = rec 
            print(f"[INFO] Added event_id: {command[3]} at pos {pos} to local DHT")
            return "Disregard"
        
        # If not, then forward to neighbor 
        n_name, n_ip, n_port = self.right_neighbor 
        store_msg = ' '.join(command)
        print(f"[SENT] Forwarded To {n_name} at {n_port}: {store_msg}")
        self.sock.sendto(store_msg.encode(), (n_ip, int(n_port)))

        # Return "Disregard" - no response needed for previous node 
        return "Disregard"

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
    




        
    

