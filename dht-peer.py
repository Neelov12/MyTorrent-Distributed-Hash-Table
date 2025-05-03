import socket
import sys
import random
import threading
import csv
import hashlib
import time
import os

# Define the allowed port range based on G = 34
# For group 34 it is 18000-18499
G = 34
PORT_RANGE = range(((G // 2) * 1000) + 1000, ((G // 2) * 1000) + 1499)

class DHTPeer: 
    def __init__(self, manager_ip, manager_port, peer_name, peer_ip, m_port, p_port):

        # Peer/Manager network information 
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.manager = (manager_ip, manager_port)
        self.peer_name = peer_name
        self.peer_ip = peer_ip
        self.m_port = m_port
        self.p_port = p_port

        # List of peers, used by leader
        self.peers = {}  # Stores peer info in the ring, 
                         # Structure: self.peers[id] = peer_name, peer_ip, peer_port 

        # Local DHT table
        self.local_dht = {}  # Local hash table for storm data,
                             # Structure: local_dht[pos] = {event_id, state, ... , tor_f_scale}
        
        # Internal DHT information, its own id, DHT ring size, and its own right/left neighbor
        self.id = None # Leader id is always set to 0
        self.ring_size = None
        self.right_neighbor = None # Structure: (n_name, n_ip, n_port)
        self.left_neighbor = None # Structure: (n_ip, n_port)

        # Information for CSV file
        self.CSV_FILE = "details-1950.csv"
        self.REQUIRED_FIELDS = [
            "EVENT_ID", "STATE", "YEAR", "MONTH_NAME", "EVENT_TYPE", "CZ_TYPE", "CZ_NAME",
            "INJURIES_DIRECT", "INJURIES_INDIRECT", "DEATHS_DIRECT", "DEATHS_INDIRECT",
            "DAMAGE_PROPERTY", "DAMAGE_CROPS", "TOR_F_SCALE"
        ]

        # Establish socket for p_port and m_port
        self.sockp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sockp.bind(("0.0.0.0", p_port))

        self.sockm = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sockm.bind(("0.0.0.0", m_port))
        
        # Internal tracking, whether peer left dht or not
        self.left_dht = False

        # Forcibly switches listening on p_port and m_port on/off
        self.listen = True

        # These two variables control what the peer is expecting to receive and do as a response
        #   This finetuning is since there may be more than one response to a message, for example "SUCCESS"
        self.waiting_on = ""
        self.waiting_to_send = ""

        # Signals that peer is waiting for a command or message 
        #   Used to reject commands from manager or other peer while it is waiting, and also to pause/resume main thread
        self.waiting = False 
        self.in_process = False
        # Statically stores port and message that peer is expecting 
        self.expecting = {} # Structure: {port, message}

        # Statically stores received message from manager or another peer 
        self.message = None

        self.packet_received = () # Format: ( (ip, port) , message)
        

# START
#   Begins multithreading
    def start(self):
        """Start listening and input threads."""
        threading.Thread(target=self.listen_on_m_port, daemon=True).start()
        threading.Thread(target=self.listen_on_p_port, daemon=True).start()
        self.input_loop()  # Run input loop on main thread (so user can Ctrl+C)

# WAIT
    # Prompts main thread to "wait" until self.waiting is set to false, this is so peer waits for a response
    # from manager before proceeding with anything else
    def wait_for(self, command, sender):
        # 'sender' format: (ip, port)
        self.packet_received = ()
        self.waiting = True
        start_time = time.time()
        timeout = 15 # Timeout in <n> seconds
        print(f"[INFO] {self.peer_name} is waiting for response {command} from {sender[0]} on port {sender[1]}")

        while True: 
            # Break loop if timed out
            if time.time() - start_time > timeout:
                print("[WARNING] Error: wait_for has timed out")
                break
            # If peer has not yet received a packet, skip to next iteration
            if not self.packet_received: 
                continue

            sender_ip = self.packet_received[0][0]
            sender_port = self.packet_received[0][1]
            sender_message = self.packet_received[1]
            command_received = sender_message.split()

            # If packet received equals the packet peer is expecting, return True so caller function can proceed
            if command == command_received[0] and sender == (sender_ip, sender_port):
                self.waiting = False
                return True
            else: 
                # If sender is different and/or message is different than expected, respond with FAILURE
                if sender_ip == manager_ip and sender_port == manager_port:
                    if command_received[0] == "FAILURE":
                        break
                    else:
                        self.sendto_manager("FAILURE waiting on a different message")
                else: 
                    self.sockp.sendto("FAILURE".encode(), (sender_ip, sender_port))
                # Reset packet_received to empty so that peer can continue waiting for a message 
                self.packet_received = ()

        # If peer has not received the expected message before timeout, return False so caller function halts process
        self.waiting = False
        return False
    
# SEND T0 MANAGER
    def sendto_manager(self, message):
        print(f"[SENT] To ({self.manager_ip} on {self.manager_port}): {message}")
        self.sockm.sendto(message.encode(), (self.manager_ip, self.manager_port))

# SEND TO RIGHT NEIGHBOR
    def sendto_r_neighbor(self, message):
        n_name, n_ip, n_port = self.right_neighbor 
        print(f"[SENT] To {n_name} ({n_ip} on {n_port}): {message}")
        self.sockp.sendto(message.encode(), (n_ip, int(n_port)))
    
# LISTEN TO MANAGER (m_port)
    def listen_on_m_port(self):
        """Continuously listen for incoming UDP messages from manager."""
        print(f"{self.peer_name} is listening to manager on port {self.m_port}...")
        while self.listen:
            try:
                data, addr = self.sockm.recvfrom(2048)
                raw_message = data.decode().strip()
                if not raw_message:
                    continue
                
                print(f"[RECEIVED] From {addr}: {raw_message}")
                
                # If peer is already in a process, it responds with a FAILURE message
                #   and no computation is done 
                if self.in_process:
                    self.sockm.sendto("FAILURE already in another process".encode(), addr)
                    continue

                # If peer is not already in another process, it saves the received message staticallay
                self.packet_received = (addr, raw_message)
                # If peer is waiting on a message, let wait_for function handle it
                if self.waiting:
                    continue

                # If peer is not waiting for anything, allow process_manager_cmd to handle message
                #   this usually indicates a new message or command
                self.process_manager_cmd(raw_message.split())

            except Exception as e:
                print("[ERROR in listen_on_m_loop]", e)

# LISTEN TO OTHER PEERS
#   Listens for commands and sends responses 
    def listen_on_p_port(self):
        """Continuously listen for incoming UDP messages."""
        print(f"{self.peer_name} is listening to other peers on port {self.p_port}...")
        while self.listen:
            try:
                data, addr = self.sockp.recvfrom(2048)
                raw_message = data.decode().strip()
                if not raw_message:
                    continue

                # Print and process all other cases
                print(f"[RECEIVED] From {addr}: {raw_message}")

                # If peer is already in a process, it responds with a FAILURE message
                #   and no computation is done 
                if self.in_process:
                    self.sockp.sendto("FAILURE already in another process".encode(), addr)
                    continue

                # If peer is not already in another process, it saves the received message staticallay
                self.packet_received = (addr, raw_message)

                # If peer is waiting on a message, let wait_for function handle it
                if self.waiting:
                    continue

                # If peer is not waiting for anything, allow process_peer_cmd to handle message
                #   this usually indicates a new message or command
                self.process_peer_cmd(raw_message.split())

                #response = self.process_command(raw_message.split())

                # Sends nothing if "Disregard", else sends response given by process_command
                #if response and response != "Disregard":
                #    print(f"[SENT] To {addr}: {response}")
                #    self.sockp.sendto(response.encode(), addr)

            except Exception as e:
                print(f"[ERROR in listen_on_p_port] {e} ({type(e)})")

# PROCESS COMMANDS FROM MANAGER
    def process_manager_cmd(self, command):
        cmd_type = command[0]

        if cmd_type == "force-exit":
            self.force_exit(command[1])

# PROCESS COMMANDS FROM OTHER PEERS 
    def process_peer_cmd(self, command):
        cmd_type = command[0]

        if cmd_type == "set-id":
            self.handle_set_id(command)
        elif cmd_type == "store":
            self.handle_store(command) 
        elif cmd_type == "teardown":
            self.handle_teardown()
        elif cmd_type == "rebuilt-dht":
            self.handle_rebuild_dht()
        elif cmd_type == "reset-id" and len(command) == 3:
            self.handle_reset_id(int(command[1]), int(command[2]))
        elif cmd_type == "rebuild-dht":
            self.handle_rebuild_dht()
        elif cmd_type == "add-me" and len(command) == 4:
            self.handle_add_me(command[1], command[2], command[3])
        elif cmd_type == "force-exit ManagerForcedExit":
            self.force_exit(command[1])

# INPUT LOOP
#   Prompts user to send a command to manager 
    def input_loop(self):
        # Prompts user to issue command to manager
        while True: 
            time.sleep(1)
            print(f"\nPeer {self.peer_name}, enter a command at any time:")
            print("1: Force Exit")
            print("2: Set up DHT (setup-dht)")
            print("3: Query DHT for event_id")
            print("4: Leave DHT (leave-dht)")
            print("5: Join DHT (join-dht)")
            print("6: Deregister and Exit (deregister)")
            print("7: Delete entire DHT (teardown-dht)")
            print("8: Print Information")
            option = input("\nSelect an option: \n").strip()

            if option == "1": 
                sys.exit() 
                break
            elif option == "2":
                n_size = input("Select size of hash table (integer): ")
                y = input("Select year of storm data (YYYY): ")
                peer.setup_dht(n_size, y)
            elif option == "3":
                event_id = input("Enter storm event ID to query: ").strip()
                self.last_queried_event_id = event_id
                peer.query_dht(event_id)
            elif option == "4":
                self.leave_dht()
                print("[INFO] DHT rebuild complete. You may now exit or rejoin another DHT.")
            elif option == "5":
                self.join_dht()
                print("[INFO] DHT rebuild complete. You've now joined the DHT.")
            elif option == "6":
                peer.deregister()
            elif option == "7":
                peer.teardown_dht()
            elif option == "8":
                peer.print_info()
            else:
                print("Invalid choice. Please enter a valid number.")

# Used to force exit program 
    def force_exit(self, exit_msg):
        print(exit_msg)
        os._exit(0)

# Print all information currently stored by peer 
    def print_info(self):
        print("INFORMATION CURRENTLY STORED:")
        print(f"\tMy name and IP: {self.peer_name}, {self.peer_ip}")
        print(f"\tMy manager is on (ip, port): {self.manager}")
        print(f"\tListening to peers on (p_port): {self.p_port}")
        print(f"\tListening to manager on (m_port): {self.m_port}")
        print("\nDHT RING INFORMATION:")
        print(f"\tMy ID: {self.id}")
        print(f"\tRing Size: {self.ring_size}")
        print(f"\tRight Neighbor: {self.right_neighbor}")
        print(f"\tLeft Neighbor: {self.left_neighbor}")
        print("\nPEERS IN DHT:")
        if self.ring_size is not None:
            for i in range(0, self.ring_size):
                peer_name, peer_ip, peer_port = self.peers[i]
                print(f"\t{i}: {peer_name} ({peer_ip}, {peer_port})")
        print("\nCURRENT NETWORK INFORMATION:")
        print(f"\tCurrently in a process: {self.in_process}")
        print(f"\tLast Received Message: {self.packet_received}")

# Send register 
    # Sends register peer_name, peer ip, m_port, p_port
    def register(self):
        """Register with the DHT manager."""
        message = f"register {self.peer_name} {self.peer_ip} {self.m_port} {self.p_port}"
        self.sendto_manager(message)

        # Listens for response from manager
        while True:
            try:
                data, addr = self.sockm.recvfrom(2048)
                raw_message = data.decode().strip()
                if not raw_message:
                    continue
 
                # Print message if received 
                print(f"[RECEIVED] From {addr}: {raw_message}")

                response = raw_message.split()
                response_type = response[0]

                if(response_type == "SUCCESS"):
                    return True 
                elif(response_type == "FAILURE"):
                    return False 
                else:
                    print("[WARNING] Unexpected response from manager after sending register")
            except Exception as e:
                print("[ERROR in register]", e)

# Send deregister
    def deregister(self):
        # After sending deregister, wait for SUCCESS then exit program
        deregister_msg = f"deregister {self.peer_name}"
        self.sendto_manager(deregister_msg)
        if self.wait_for("SUCCESS", self.manager):
            print("[INFO] Successfully deregistered.")
            self.force_exit("Exiting...")
        else: 
            print("[INFO] Could not deregister.")

# Send setup-dht
    # (peer_name, n, yyyy)
    def setup_dht(self, n, year):
        """Send setup-dht request to the manager."""
        message = f"setup-dht {self.peer_name} {n} {year}"
        self.sendto_manager(message)

        if self.wait_for("SUCCESS", self.manager):
            self.in_process = True
            self.initialize_ring(self.packet_received[1])
        else: 
            print("[INFO] Could not do setup-dht")

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
        self.set_ids()

# Send set-id
    # Assign ids to all the peers 
    def set_ids(self):
        """Assign IDs to peers and establish the ring topology."""
        # First, set own id, ring size, and right neighbor 
        self.id = 0  # Leader ID
        n = len(self.peers)
        self.ring_size = n
        next_id = self.id+1 % self.ring_size
        self.right_neighbor = self.peers[self.id+1 % self.ring_size]
        # Set left neighbor (this is the peer who leader receives messages from in ring)
        prev_id = (self.id - 1) % self.ring_size
        n_name, n_ip, n_port = self.peers[prev_id]
        self.left_neighbor = (n_ip, int(n_port))
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
            self.sockp.sendto(set_id_msg.encode(), (peer_ip, int(peer_port)))
        self.construct_dht("setup-dht")
    
    # Construct DHT, send store commands to right neighbor
    def construct_dht(self, functionality): 
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

        # Function can be called in process of different commands
        if functionality == "setup-dht":
            self.dht_complete()
        elif functionality == "leave-dht":
            print("[INFO] DHT successfully reconstructed")

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
    
# Send store 
    def store(self, peer_id, pos, rec):
        """Used by leader to send store command to right neighbor."""
        if peer_id not in self.peers:
            print(f"[WARNING] Peer ID {peer_id} not found in ring.")
            return
        
        rec_data = " ".join(rec)
        store_msg = f"store {peer_id} {pos} {rec_data}"
        self.sendto_r_neighbor(store_msg)

# Send dht-complete <peer_name>
    def dht_complete(self):
        self.in_process = False
        dht_complete_msg = f"dht-complete {self.peer_name}"
        self.sendto_manager(dht_complete_msg)

        if self.wait_for("SUCCESS", self.manager):
            print("[INFO] DHT has been successfully completed.")
        else: 
            print("[INFO] Error setting up DHT.")


# Handles set-id 
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
        self.right_neighbor = self.peers[next_id] # (peer_name, ip, port)
        # Set left neighbor (this is the peer who this peer receives messages from)
        prev_id = (self.id - 1) % self.ring_size
        n_name, n_ip, n_port = self.peers[prev_id]
        self.left_neighbor = (n_ip, int(n_port))

        print(f"[INFO] Assigned ID: {self.id}")
        print(f"[INFO] Ring size: {self.ring_size}")
        print(f"[INFO] Right neighbor (ID {next_id}): {self.right_neighbor}")

# Handles store 
    def handle_store(self, command):
        # store command structure = store <peer_id> <pos> <rec> 
        peer_id = int(command[1])        
        pos = int(command[2])             
        rec = tuple(command[3:])

        # If store is intended for this peer 
        if self.id == peer_id:
            self.local_dht[pos] = rec 
            print(f"[INFO] Added event_id: {command[3]} at pos {pos} to local DHT")
            return 
        
        # If not, then forward to neighbor 
        store_msg = ' '.join(command)
        self.sendto_r_neighbor(store_msg)

# Sends query-dht  
    def query_dht(self, event_id):
        msg = f"query-dht {self.peer_name}"
        self.sock.sendto(msg.encode(), (self.manager_ip, self.manager_port))

        try:
            response, _ = self.sock.recvfrom(2048)
            parts = response.decode().split()
            if parts[0] != "SUCCESS":
                print("[FAILURE] Query rejected by manager:", response.decode())
                return

            entry_peer_name = parts[1]
            entry_ip = parts[2]
            entry_pport = int(parts[3])

            msg = f"find-event {event_id} {self.peer_name} {self.peer_ip} {self.p_port} 0"
            print(f"[SENT] Query sent to {entry_peer_name} at {entry_ip}:{entry_pport}")
            print(f"[DEBUG] Sending find-event to {entry_ip}:{entry_pport} => {msg}")
            self.sock.sendto(msg.encode(), (entry_ip, entry_pport))
        except Exception as e:
            print("[ERROR] Failed to send query:", e)


    def handle_find_event(self, command):
        event_id = int(command[1])
        origin_name = command[2]
        origin_ip = command[3]
        origin_port = int(command[4])
        hops = int(command[5])

        pos = event_id % self.next_prime(2 * 1000)
        target_id = pos % self.ring_size

        if self.id == target_id:
            record = self.local_dht.get(pos)
            if record:
                response = f"SUCCESS {event_id} hops={hops} record=" + "|".join(record)
            else:
                response = f"FAILURE {event_id} hops={hops} record=NOT_FOUND"
            print(f"[RESULT] Sent back to {origin_name} at {origin_ip}:{origin_port}")
            self.sock.sendto(response.encode(), (origin_ip, origin_port))
            return "Disregard"
        else:
            next_id = random.choice([i for i in self.peers if i != self.id])
            peer_name, peer_ip, peer_port = self.peers[next_id]
            if int(peer_port) == self.manager_port:
                print("[WARN] Not forwarding to manager (invalid peer)")
                return "Disregard"
            hops += 1
            forward_msg = f"find-event {event_id} {origin_name} {origin_ip} {origin_port} {hops}"
            print(f"[FORWARD] To {peer_name} at {peer_ip}:{peer_port} (hop {hops})")
            self.sock.sendto(forward_msg.encode(), (peer_ip, int(peer_port)))
            return "Disregard"
            
# Sends leave-dht <peer_name> 
#   to manager and waits for SUCCESS, then does
#   Step 1 of leave-dht, sends teardown to cycle thru ring
    def leave_dht(self):
        message = f"leave-dht {self.peer_name}"
        # After sending leave-dht, waits for SUCCESS then sends teardown to right neighbor 
        self.sendto_manager(message)
        if self.wait_for("SUCCESS", self.manager):
            self.teardown("leave-dht")
        else: 
            print("[INFO] Manager does not approve leave-dht ")
    
    # Step 2 of leave-dht
    #   Only used by leaving peer 
    def reset_id(self):
        # Deletes local DHT
        self.local_dht = {}
        # Sends reset-id <new_peer_id> <leaving_peer_id> to right neighbor
        reset_id_msg = f"reset-id 0 {self.id}"
        self.sendto_r_neighbor(reset_id_msg)
        # Leaving peer now waits for reset-id to cycle back to itself
        if self.wait_for("reset-id", self.left_neighbor):
            self.rebuild_dht("leave-dht")
        else:
            print("[INFO] Critical Error: reset-id never sent back")
    
    #   Handle reset-id command from left neighbor
    def handle_reset_id(self, new_id, leaving_id):
        # Set new peer id and new neighbor id 
        old_id = self.id
        self.id = int(new_id)
        new_neighbor_id = self.id + 1
        # Set ring size - 1
        old_ring_size = self.ring_size
        new_ring_size = old_ring_size - 1
        self.ring_size = new_ring_size
        # Adjust peer tuples, removing leaving peer and reorder self.peers
        new_peers = {}
        for i in range(new_ring_size):
            newer_id = i
            older_id = (leaving_id + i + 1) % old_ring_size
            new_peers[newer_id] = self.peers[older_id]
        self.peers = new_peers
        # For now, leave right neighbor the same as reset-id has to be forwarded back to leaving peer 
        # Forward reset-id to right neighbor
        reset_id_msg = f"reset-id {new_neighbor_id} {leaving_id}"
        self.sendto_r_neighbor(reset_id_msg)
        # Now, change right neighbor so that no more messages are sent to the leaving peer
        n_name, n_ip, n_port = self.right_neighbor 
        test = self.id+1 % self.ring_size
        self.right_neighbor = self.peers[(self.id+1) % self.ring_size]     
        # Print summary of changes
        print(f"[SUMMARY] My information has been changed in the following way:")
        print(f"            - ID: {old_id} -> {new_id}")
        print(f"            - Ring size: {old_ring_size} -> {new_ring_size}")   
        print(f"            - Right neighbor: {n_name} -> {self.right_neighbor[0]}")   
    
    # Step 3 of leave-dht
    def rebuild_dht(self, functionality):
        # Send to right neighbor
        rebuild_dht_msg = f"rebuild-dht"
        self.sendto_r_neighbor(rebuild_dht_msg)     
        # Wait for SUCCESS from right neighbor, new leader
        n_name, n_ip, n_port = self.right_neighbor 
        if self.wait_for("SUCCESS", (n_ip, int(n_port))):
            self.dht_rebuilt(functionality)
        else:
            print("[INFO] New leader could not rebuild DHT")
    
    # Meant only for new leader
    def handle_rebuild_dht(self):
        # Respond back to leaving peer 
        self.construct_dht("leave-dht")
        # Send SUCCESS back to leaving peer
        message = "SUCCESS"
        print(f"[SENT] To {self.left_neighbor}: SUCCESS")
        self.sockp.sendto(message.encode(), self.left_neighbor)
        # Finally, set left neighbor to peer at id ring size - 1, 
        #   P2P messages can no longer be sent to leaving peer 
        prev_id = (self.id - 1) % self.ring_size
        n_name, n_ip, n_port = self.peers[prev_id]
        self.left_neighbor = (n_ip, int(n_port))
    
    # Step 4 of leave-dht 
    def dht_rebuilt(self, functionality):
        if functionality == "leave-dht":
            # Sets the new leader
            new_leader_name = self.right_neighbor[0] 
            # Resets all DHT P2P variables so it is ready if peer wants to rejoin DHT 
            # List of peers, used by leader
            self.peers = {}         
            self.local_dht = {}  
            self.id = None 
            self.ring_size = None
            self.right_neighbor = None 
            self.left_neighbor = None
            # Sets left_dht to True so it is tracked that this peer has at one point left a DHT
            self.left_dht = True        
        # Sends dht-rebuilt <new_leader_name>
        dht_rebuilt_msg = f"dht-rebuilt {self.peer_name} {new_leader_name}"
        self.sendto_manager(dht_rebuilt_msg)
        if self.wait_for("SUCCESS", self.manager):
            if functionality == "leave-dht":
                print("[INFO] Successfully left DHT")
            elif functionality == "join-dht":
                print("[INFO] Successfully joined DHT")
        else: 
            print("[INFO] Changed the entire ring, but does not approve me leaving/joining DHT... we're in trouble")

# join-dht <peer_name>
    # Send join-dht to manager
    def join_dht(self):
        # Send command to manager
        join_dht_msg = f"join-dht {self.peer_name}"
        self.sendto_manager(join_dht_msg)
        # Wait for SUCCESS 
        if self.wait_for("SUCCESS", self.manager):
            self.add_me(self.packet_received[1])
        else: 
            print("[INFO] Manager does not approve join-dht")
    
    # Configure self dht info then send add-me to leader 
    def add_me(self, response):
        # Parse response from manager,
            # format: "SUCCESS <list of peers in DHT>"
        lines = response.split(' ')[2:]
        # Create list of peers already in dht
        self.peers = {
            i: tuple(
            val for j, val in enumerate(lines[i].strip('()').split(',')) if j != 2)
            for i in range(len(lines))
        }
        # Add self to list of peers in dht 
        my_id = len(self.peers)
        self.peers[my_id] = (self.peer_name, self.peer_ip, self.p_port)
        # Set own dht info 
        self.id = my_id
        self.ring_size = my_id+1
        self.right_neighbor = self.peers[0]
        prev_id = (self.id - 1) % self.ring_size
        n_name, n_ip, n_port = self.peers[prev_id]
        self.left_neighbor = (n_ip, int(n_port))
        print(f"id: {self.id}, ring size: {self.ring_size}")
        # Print updated list
        print("\n[SUMMARY] Records saved:")
        for peer_id in range(self.ring_size):
            peer_name, peer_ip, peer_port = self.peers[peer_id]
            print(f"Peer ID {peer_id} ({peer_name} at {peer_ip}:{peer_port})")
        # Send add-me command to right neighbor (current leader)
        #self.sendto_r_neighbor(f"add-me {self.peer_name} {self.peer_ip} {self.p_port}")
        # Wait for command to cycle the ring
        if self.wait_for("add-me", self.left_neighbor):
            self.teardown("join-dht")
        else: 
            print("[INFO] Error: addme did not cycle through ring")
        
    # Handle addme command from joining peer
    def handle_add_me(self, peer_name, peer_ip, peer_port):
        old_last_id = len(self.peers)-1
        # Change ring_size 
        old_r_size = self.ring_size
        self.ring_size = old_r_size + 1
        # Add peer to self.peers
        new_peer_id = old_r_size
        self.peers[new_peer_id] = (peer_name, peer_ip, peer_port)
        # If leader, change left neighbor to new peer
        if self.id == 0:
            n_name, n_ip, n_port = self.peers[new_peer_id]
            self.left_neighbor = (n_ip, int(n_port))
        # If originally at the end of ring (max ID), change right neighbor to new peer
        if self.id == old_last_id:
            self.right_neighbor = self.peers[new_peer_id]
        # Send add-me command to right neighbor 
        self.sendto_r_neighbor(f"add-me {peer_name} {peer_ip} {p_port}")
        # Print updated list
        print("\n[SUMMARY] Records saved:")
        for peer_id in range(self.ring_size):
            peer_name, peer_ip, peer_port = self.peers[peer_id]
            print(f"Peer ID {peer_id} ({peer_name} at {peer_ip}:{peer_port})")

# Send teardown_dht <peer_name>
#   Used by leader to tear down the entire DHT
    def teardown_dht(self):
        if self.id != 0:
            print("[WARNING] You are not the leader")

        # Sends teardown-dht <peer_name> command to manager 
        teardown_to_man_msg = f"teardown-dht {self.peer_name}"
        self.sendto_manager(teardown_to_man_msg)
        # Waits for SUCCESS from manager, sends teardown to right neighbor if received
        if self.wait_for("SUCCESS", self.manager):
            self.teardown("teardown-dht")
        else: 
            print("[INFO] Manager does not approve teardown-dht")

# Sends teardown
    def teardown(self, functionality):
        # Sends teardown to right neighbor
        self.sendto_r_neighbor("teardown")
        # Waits for teardown command once the message is cycled thru the ring 
        #   then, sends teardown-complete to manager or deletes own dht 
        if self.wait_for("teardown", self.left_neighbor):
            if functionality == "teardown-dht":
                self.teardown_complete()
            elif functionality == "leave-dht":
                self.reset_id()
            elif functionality == "join-dht":
                self.rebuild_dht(functionality)
        else: 
            print("[INFO] Did not receive teardown command from left neighbor")

# Handles teardown
    def handle_teardown(self):
        self.local_dht = {} # Delete local DHT
        # Send teardown to right neighbor
        self.sendto_r_neighbor("teardown")

# Sends teardown_complete <peer_name> to manager
    def teardown_complete(self):
        # Leader deletes own local DHT 
        self.local_dht = {}
        # Sends teardown-complete <peer_name> command to manager 
        teardown_to_man_msg = f"teardown-complete {self.peer_name}"
        self.sendto_manager(teardown_to_man_msg)

        # Wait for SUCCESS response
        if self.wait_for("SUCCESS", self.manager):
            print("[INFO] DHT successfully torn down")
        else: 
            print("[INFO] DHT torn down but manager does not approve. We're in trouble.")

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

    peer = None
    peer_name = None
    m_port = None
    p_port = None

    print("BECOME A PEER")
    registered = False
    configuring = True 
    while not registered:
        while configuring:
            # Takes peer name as input, throws error if 15 > characters
            peer_name = input("Enter peer name (max 15 characters): ").strip()
            while len(peer_name) > 15 or not peer_name.isalpha():
                print("Peer name must be alphabetic and at most 15 characters.")
                peer_name = input("Enter peer name: ").strip()

            # Takes manager and peer port and requests again if either is not in range
            m_port = int(input("Enter manager port (m_port): ").strip())
            p_port = int(input("Enter peer-to-peer port (p_port): ").strip())
            while m_port not in PORT_RANGE or p_port not in PORT_RANGE or m_port == p_port:
                print("Ports must be integers and must be in the range 18000 - 18499, and they cannot be the same.\n")
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
                peer = DHTPeer(manager_ip, manager_port, peer_name, peer_ip, m_port, p_port)
                print("\nContinuing...")
            else: 
                continue

        confirmation = input("\nWould you like to register? (Y/N): ").strip().lower()
        if confirmation == "y":
            registered = peer.register()
            if registered == False:
                print("\nIt looks like registration failed. Please refer to the message sent by manager and reconfigure.")
                configuring = True
            else:
                print("\nContinuing...")
        else:        
            print("No commands can be sent to manager before registration. Rerequesting...")


    # Starts multithreading socket and input from user 
    peer.start() 
