import socket
import random
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
        self.peers = {}  # Stores peer information {peer_name: (ip, m_port, p_port, state)}
        self.dht = None  # Stores current DHT setup
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        print(f"The IP address of the DHT Manger is: {socket.gethostbyname(socket.gethostname())}") 
        print(f"DHT Manager listening on port {port}...")
    
    def run(self):
        while True:
            data, addr = self.sock.recvfrom(1024)
            command = data.decode().split()
            if not command:
                continue
            
            response = self.process_command(command)
            self.sock.sendto(response.encode(), addr)
    
    def process_command(self, command):
        cmd_type = command[0]
        
        if cmd_type == "register" and len(command) == 5:
            return self.handle_register(command[1], command[2], int(command[3]), int(command[4]))
        elif cmd_type == "setup-dht" and len(command) == 4:
            return self.setup_dht(command[1], int(command[2]), command[3])
        elif cmd_type == "dht-complete" and len(command) == 2:
            return self.dht_complete(command[1])
        else:
            return "FAILURE Invalid command"
    
    # REGISTER (Receive) 
    def handle_register(self, peer_name, ip, m_port, p_port):
        # Output packet sent and received 
        print(f"[REGISTER] Received from {peer_name}, {ip}, on p2p port {p_port}]")
        if peer_name in self.peers or any(p[1] == ip and (p[2] == m_port or p[3] == p_port) for p in self.peers.values()):
            print("SENT: FAILURE Duplicate peer or port conflict")
            return "FAILURE Duplicate peer or port conflict"
        
        self.peers[peer_name] = (ip, m_port, p_port, "Free")
        print("SENT: SUCCESS")
        return "SUCCESS"
    
    def setup_dht(self, leader, n, year):
        if leader not in self.peers or self.peers[leader][3] != "Free":
            return "FAILURE Leader not valid"
        if n < 3:
            return "FAILURE n must be at least 3"
        if len(self.peers) < n:
            return "FAILURE Not enough peers"
        if self.dht is not None:
            return "FAILURE DHT already exists"
        
        available_peers = [p for p in self.peers if self.peers[p][3] == "Free"]
        selected_peers = random.sample(available_peers, n - 1)
        self.peers[leader] = (*self.peers[leader][:3], "Leader")
        
        for p in selected_peers:
            self.peers[p] = (*self.peers[p][:3], "InDHT")
        
        self.dht = [leader] + selected_peers
        dht_info = [(p, *self.peers[p][:3]) for p in self.dht]
        return "SUCCESS " + " ".join(["(" + ",".join(map(str, peer)) + ")" for peer in dht_info])
    
    def dht_complete(self, peer_name):
        if self.dht is None or self.dht[0] != peer_name:
            return "FAILURE Not the leader"
        return "SUCCESS"
    
if __name__ == "__main__":
    # port = int(input("Enter manager port number: (Range is 18000-18499)"))
    if len(sys.argv) != 2:
        print("Usage: python dht-peer.py <manager_port (18000-18499)>")
        sys.exit(1)
    port = int(sys.argv[1])
    manager = DHTManager(port)
    manager.run()