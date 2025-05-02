import socket
import random
import threading
import sys

# Define the allowed port range based on G = 34
# For group 34 it is 18000-18499
G = 34
PORT_RANGE = range(((G // 2) * 1000) + 1000, ((G // 2) * 1000) + 1499)

class DHTManager:
    def __init__(self, port):
        if port not in PORT_RANGE:
            raise ValueError(f"Port {port} is out of the allowed range {PORT_RANGE}")
        
        self.port = port
        self.peers = {}  # Stores peer information {peers[peer_name]: (ip, m_port, p_port, state)}
        self.dht = None  # Stores the peer_name of all peers in current DHT 
        self.leaving_peer = None
        self.joining_peer = None
    
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        print(f"The IP address of the DHT Manger is: {socket.gethostbyname(socket.gethostname())}") 
        print(f"DHT Manager listening on port {port}...")

        # Toggles listen_loop listener on/off
        #   Usually used to stop listening to new commands, returning FAILURE otherwise
        self.listen = True

        # Sets what command the manager is waiting for, manager sends FAILURE if received command does not match waiting_on
        #   Empty string indicates that the manager is not waiting on any specific command
        self.waiting_on = ""

# LISTEN LOOP
    # Manages listens and responses 
    def listen_loop(self):
        """Continuously listen for incoming UDP messages."""
        while self.listen:
            try:
                data, addr = self.sock.recvfrom(1024)
                command = data.decode().split()
                if not command:
                    continue

                response = self.process_command(command)
                print(f"[RECEIVED] From {addr}: {' '.join(command)}")
                print(f"[SENT] {response}\n")
                if response != "NO RESPONSE":
                    self.sock.sendto(response.encode(), addr)
            except Exception as e:
                print("[ERROR in listen_loop]", e)

# INPUT LOOP 
    def input_loop(self):
        """Handle user input for sending messages or exiting."""
        while True:
            print("\nEnter '1' at any time to exit")
            print("\nEnter '2' at any time to print information")
            choice = input("").strip()

            if choice == "1":
                print("Exiting program...")
                # Sends command to all registered peers to also exit
                message = "force-exit ManagerForcedExit"
                for peer_name, (ip, m_port, p_port, state) in self.peers.items():
                    self.sock.sendto(message.encode(), (ip, m_port))
                exit(0)
            if choice == "2": 
                print("REGISTERED PEERS WITH MANAGER:")
                print(self.peers)
                print("PEERS THAT ARE IN THE DHT:")
                print(self.dht)
            else:
                print("Invalid choice. Please enter 1 to exit.")

# START
    def start(self):
        """Start listening and input threads."""
        threading.Thread(target=self.listen_loop, daemon=True).start()
        self.input_loop()  # Run input loop on main thread (so user can Ctrl+C)
    
# PROCESS COMMAND
    # Manages responses to commands 
    def process_command(self, command):
        cmd_type = command[0]
        
        # Checks if manager is waiting on a specific command
        #   if it is but incoming command does not match the message it's waiting on, it returns FAILURE
        if self.waiting_on != "":
            if cmd_type != self.waiting_on:
                return f"FAILURE Manager is waiting for {self.waiting_on}"
        
        if cmd_type == "register" and len(command) == 5:
            return self.handle_register(command[1], command[2], int(command[3]), int(command[4]))
        elif cmd_type == "setup-dht" and len(command) == 4:
            return self.handle_setup_dht(command[1], int(command[2]), command[3])
        elif cmd_type == "dht-complete" and len(command) == 2:
            return self.handle_dht_complete(command[1])
        elif cmd_type == "query-dht" and len(command) == 2:
            return self.handle_query_dht(command[1])
        elif cmd_type == "leave-dht" and len(command) == 2:
            return self.handle_leave_dht(command[1])
        elif cmd_type == "dht-rebuilt" and len(command) == 3:
            return self.handle_dht_rebuilt(command[1], command[2])
        elif cmd_type == "join-dht" and len(command) == 2:
            return self.handle_join_dht(command[1])
        elif cmd_type == "deregister" and len(command) == 2:
            return self.handle_deregister(command[1])
        elif cmd_type == "teardown-dht" and len(command) == 2: 
            return self.handle_teardown_dht(command[1])
        elif cmd_type == "teardown-complete" and len(command) == 2: 
            return self.handle_teardown_complete(command[1])
        elif cmd_type == "FAILURE": 
            return "NO RESPONSE"
        else:
            return "FAILURE Invalid command"
    
# Handle register (Receive) 
    def handle_register(self, peer_name, ip, m_port, p_port):
        # Output packet sent and received 
        if peer_name in self.peers or any(p[1] == ip and (p[2] == m_port or p[3] == p_port) for p in self.peers.values()):
            return "FAILURE Duplicate peer or port conflict"
        
        self.peers[peer_name] = (ip, m_port, p_port, "Free")
        return "SUCCESS"
    
# Handle setup-dht    
    def handle_setup_dht(self, leader, n, year):
        if leader not in self.peers or self.peers[leader][3] != "Free":
            return "FAILURE Leader not valid"
        if n < 3:
            return "FAILURE n must be at least 3"
        if len(self.peers) < n:
            return "FAILURE Not enough peers"
        if self.dht is not None:
            return "FAILURE DHT already exists"
        
        # Determine peers that are available to be in DHT minus the leader (since they are already in DHT)
        available_peers = [p for p in self.peers if self.peers[p][3] == "Free" and p != leader]
        # Randomly select n-1 available peers 
        selected_peers = random.sample(available_peers, n - 1)
        # Set leader peer to state "Leader"
        ip, m_port, p_port, _ = self.peers[leader]  # unpack everything except old state
        self.peers[leader] = (ip, m_port, p_port, "InDHT")
        
        # Set all selected peers to state "InDHT"
        for p in selected_peers:
            ip, m_port, p_port, _ = self.peers[p]
            self.peers[p] = (ip, m_port, p_port, "InDHT")
        
        self.dht = [leader] + selected_peers
        dht_info = [(p, *self.peers[p][:3]) for p in self.dht]
        return "SUCCESS 2 " + " ".join(["(" + ",".join(map(str, peer)) + ")" for peer in dht_info])
    
# Handle dht-complete
    def handle_dht_complete(self, peer_name):
        if self.dht is None or self.dht[0] != peer_name:
            return "FAILURE Not the leader"
        for p in self.dht:
            self.peers[p] = (*self.peers[p][:3], "Free")
        return "SUCCESS 4"

# Handle query-dht
    def handle_query_dht(self, peer_name):
        if self.dht is None:
            return "FAILURE DHT not setup"
        if peer_name not in self.peers:
            return "FAILURE Peer not registered"
        if self.peers[peer_name][3] != "Free":
            return "FAILURE Peer not in Free state"
    
        # Choose a random peer in the DHT to start the query
        selected = random.choice(self.dht)
        selected_info = self.peers[selected]
        return f"SUCCESS {selected} {selected_info[0]} {selected_info[2]}"
    
# Handle leave-dht 
    def handle_leave_dht(self, peer_name):
        if self.dht is None or peer_name not in self.dht:
            return "FAILURE Peer not in DHT"
        if self.leaving_peer is not None:
            return "FAILURE Another leave/join already in progress"
        
        self.leaving_peer = peer_name
        self.waiting_on = "dht-rebuilt"
        return "SUCCESS"

# Handle dht-rebuilt
    def handle_dht_rebuilt(self, peer_name, new_leader):
        
        if self.joining_peer is not None:
            if peer_name != self.joining_peer:
                return "FAILURE Rebuilder mismatch"
            if new_leader not in self.dht:
                return "FAILURE New leader not valid"
            
            self.waiting_on = ""

            self.peers[self.joining_peer] = (*self.peers[self.joining_peer][:3], "InDHT")
            self.dht.append(self.joining_peer)
            self.peers[new_leader] = (*self.peers[new_leader][:3], "Leader")

            for p in self.dht:
                if p != new_leader and p != self.joining_peer:
                    self.peers[p] = (*self.peers[p][:3], "InDHT")

            self.dht = [new_leader] + [p for p in self.dht if p != new_leader]
            self.joining_peer = None
            return "SUCCESS"
        
        if self.leaving_peer is None:
            return "FAILURE No leave/join in progress"
        if peer_name != self.leaving_peer:
            return "FAILURE Rebuilder mismatch"
    
        if new_leader not in self.dht:
            return "FAILURE New leader not valid"

        self.dht.remove(self.leaving_peer)
        self.peers[self.leaving_peer] = (*self.peers[self.leaving_peer][:3], "Free")
        self.peers[new_leader] = (*self.peers[new_leader][:3], "Leader")
    
        for p in self.dht:
           if p != new_leader:
                self.peers[p] = (*self.peers[p][:3], "InDHT")

        self.dht = [new_leader] + [p for p in self.dht if p != new_leader]
        self.leaving_peer = None
        return "SUCCESS"
        
# Handle join-dht
    def handle_join_dht(self, peer_name):
        if self.dht is None:
            return "FAILURE No DHT exists"
        if peer_name not in self.peers or self.peers[peer_name][3] != "Free":
            return "FAILURE Peer not in Free state"
        if self.joining_peer is not None or self.leaving_peer is not None:
            return "FAILURE Another leave/join already in progress"

        self.joining_peer = peer_name
        return "SUCCESS"

# Handle deregister
    def handle_deregister(self, peer_name):
        if peer_name not in self.peers:
            return "FAILURE Peer not registered"
        if self.peers[peer_name][3] != "Free":
            return "FAILURE Peer must be Free to deregister"
        del self.peers[peer_name]
        return "SUCCESS"
    
# Handle teardown_dht <peer_name>     
    def handle_teardown_dht(self, peer_name):
        # Checks if peer is leader
        if self.peers[peer_name][3] != "Leader":
            return "FAILURE Peer must be a Leader"
        else: 
            # Set manager waiting on command to teardown-complete so it sends FAILURE to all other incoming messages
            self.waiting_on = "teardown-complete"
            # Return SUCCESS so leader can proceed with teardown
            return "SUCCESS"         

# Handle teardown_complete <peer_name> 
    def handle_teardown_complete(self, peer_name):
        # Checks if peer is leader
        if self.peers[peer_name][3] != "Leader":
            return "FAILURE Peer must be a Leader"
        else:
            # Reset self.waiting_on so manager can start processing other commands
            self.waiting_on = ""    
            # Set each peer in DHT's state to Free
            for peer_name in self.dht:
                peer_ip, m_port, p_port, _ = self.peers[peer_name]  # Unpack existing values
                self.peers[peer_name] = (peer_ip, m_port, p_port, "Free")  # Update state
            # Return SUCCESS message
            return "SUCCESS"


    
if __name__ == "__main__":
    # port = int(input("Enter manager port number: (Range is 18000-18499)"))
    if len(sys.argv) != 2:
        print("Usage: python dht-peer.py <manager_port (18000-18499)>")
        sys.exit(1)
    port = int(sys.argv[1])
    manager = DHTManager(port)
    manager.start()
