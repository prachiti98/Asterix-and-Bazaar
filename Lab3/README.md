To run the synchronous trader network:
python3 peer.py  ‘Synchronous’

To run the cached trader network:
python3 peer.py  ‘Cached’

To run the testcases:
python3  test.py ‘Cached’ #testNo

To run the fault tolerance testcases:
1)python3  fault_tolerance.py ‘Cached’ ‘T’ 
2)python3  fault_tolerance.py ‘Cached’ ‘F’ 


Important Notes: 
Peer_0.txt is the data warehouse
Usually Peer_1.txt and Peer_2.txt are traders
And rest are buyers or sellers
The db_load variable can be changed to change the configuration of the network.
