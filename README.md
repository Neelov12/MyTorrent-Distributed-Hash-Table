# Distributed Hash Table 

This Distributed Hash Table (DHT) uses a secure hash function based on the number of peers available to fetch data from large datasets. The DHT topology depends on a Manager-Leader-Peer relationship, where the Manager builds, organizes, and maintains the DHT, the Leader is a Peer with exclusive permission to communicate only with the Manager, and the Peers are designated only to communicate P2P and with no other host. 

What makes this DHT stand out is its flexibility: the DHT is designed with a host of protocols that allow Peers to leave and join the DHT at any time, and allow new volunteers to join the topology with Manager approval. These protocols enable a safe, reliable operation in case of crashes, system errors, or volunteer opt-outs, with minimal to no delays in response time, allowing developers to optimize the topology to larger-scaled projects such as Torrents. 

## Commands 

| Name | Format | 
| ------- | ------ | 
| register | `register {peer-name} {IPv4 address} {m-port} {p-port}` | 
| setup-dht | `setup-dht {peer-name} {n}` | 
| query-dht | `query-dht` | 
| find-event | `find-event {event_id}` | 
| leave-dht | `leave-dht {peer-name}` | 
| join-dht | `join-dht {peer-name}` | 
| teardown-dht | `teardown-dht` | 


