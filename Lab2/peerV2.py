from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc
import time
import threading as td
import socket,socketserver
import random
import numpy as np
import sys
import json
import csv
import shutil
import os.path
import json

#What are local events? (Only buy)
#When A sends again check trader clock if other buyers have completed just as many buys or else wait till they do



# Log a transaction
def log_transaction(filename,log):
    log = json.dumps(log)
    with open(filename,'a', newline='') as csvF:
        csvWriter = csv.writer(csvF,delimiter = ' ')
        csvWriter.writerow([log])
        
# Mark Transaction Complete        
def mark_transaction_complete(filename,transaction,identifier):
    with open(filename, 'r', newline='') as csvFile:
        reader = csv.reader(csvFile, delimiter=' ')
        for row in reader:
            row = json.loads(row[0])
            for i,j in row.items():
                k,_ = i,j
            if k == identifier:
                row[k]['completed'] = True
            row = json.dumps(row)
    with open(filename,'a', newline='') as csvF:
        csvWriter = csv.writer(csvF,delimiter = ' ')
        csvWriter.writerow([row])

# Log the seller info 
def seller_log(trade_list):
    with open('seller_info.csv','w', newline='') as csvF:
        csvWriter = csv.writer(csvF,delimiter = ' ')
        for k,v in trade_list.items():
            log = json.dumps({k:v})
            csvWriter.writerow([log])  
            
# Read the seller log           
def read_seller_log():
    with open('seller_info.csv','r', newline='') as csvF:
        seller_log = csv.reader(csvF,delimiter = ' ')
        dictionary = {}
        for log in seller_log:
            log = json.loads(log[0])
            for i,j in log.items():
                k,v = i,j
            dictionary[k] = v
    return dictionary

# Return any unserved requests.    
def get_unserved_requests():
    with open('transactions.csv','r', newline='') as csvF:
        transaction_log = csv.reader(csvF,delimiter = ' ')
        open_requests = []
        transaction_list = list(transaction_log)
        print(transaction_list[len(transaction_list)-1])
        last_request = json.loads(transaction_list[len(transaction_list)-1][0])
        for i,j in last_request.items():
            _,v = i,j
        if v['completed'] == False:
            return last_request
        else:
            return None
# Multi-Threaded RPC Server.
class AsyncXMLRPCServer(socketserver.ThreadingMixIn,SimpleXMLRPCServer): pass

# Peer
class peer:
    def __init__(self,host_addr,peer_id,neighbors,db,totalPeers):
        self.host_addr = host_addr
        self.peer_id = peer_id
        self.neighbors = neighbors
        self.db = db 
        self.trader = {} 
       
        # Shared Resources
        self.didReceiveOK = False # Flag
        self.didReceiveWon = False # Flag
        self.isElectionRunning = False # Flag
        self.trade_list = {} 
        
        # Vector Clock.
        self.clock  =  {i:0 for i in range(1,totalPeers+1)}
        
        # Trade Counter
        self.trade_count = 0
       
        # Semaphores 
        self.flag_won_semaphore = td.BoundedSemaphore(1) 
        self.trade_list_semaphore = td.BoundedSemaphore(1)
        self.clock_semaphore = td.BoundedSemaphore(1)       
        
   # Helper Method: Returns the proxy for specified address.
    def get_rpc(self,neighbor):
        a = xmlrpc.client.ServerProxy('http://' + str(neighbor) + '/')
            
        # Just in case the method is registered in the XmlRPC server
        return True, a

    def clock_forward(self,node_type,node_id):
        if node_type == 'Trader':
            self.clock_semaphore.acquire()
            self.clock[self.peer_id]+=1
            self.clock_semaphore.release()
        else:
            self.clock_semaphore.acquire()
            self.clock[node_id]+=1
            self.clock_semaphore.release()

    # Starting Server    
    def startServer(self):
        # Start Server and register its functions.
        host_ip = '127.0.0.1'
        server = AsyncXMLRPCServer((host_ip,int(self.host_addr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.transaction,'transaction')
        server.register_function(self.election_message,'election_message')
        server.register_function(self.register_products,'register_products')
        server.register_function(self.election_restart_message,'election_restart_message')
        server.serve_forever()
   
    # election_restart_message : Once a peer recieves this messages, it indicates that a new election will be started.
    # Subsequent action is to set election running flag up.
    # This flag indicates to buyer who wants to buy, to wait till the election to restart the buying process.
    def election_restart_message(self):
        self.flag_won_semaphore.acquire()
        self.isElectionRunning = True
        if self.db['Role'] == "Trader": 
            if len(self.db['shop'])!= 0:
                self.db['Role'] = "Buyer"
            else:
                self.db['Role'] = "Seller"
        self.flag_won_semaphore.release()
        
    # Helper method : To send election restart messages to a peer on a new thread.      
    def send_restart_election_messages(self,_,neighbor):
        connected,proxy = self.get_rpc(neighbor['host_addr'])
        if connected:
            proxy.election_restart_message() 
            
     # Helper method : To send election message to a peer on a new thread.                
    def send_message(self,message,neighbor):
        connected,proxy = self.get_rpc(neighbor['host_addr'])
        if connected:
            proxy.election_message(message,{'peer_id':self.peer_id,'host_addr':self.host_addr})
            
    # Helper method : To send the flags and send the "I won" message to peers.
    def fwd_won_message(self):
        print("Dear buyers and sellers, My ID is ",self.peer_id, "and I am the new coordinator")
        self.didReceiveWon = True
        self.trader = {'peer_id':self.peer_id,'host_addr':self.host_addr}
        self.db['Role'] = 'Trader'
        self.flag_won_semaphore.release()
        for neighbor in self.neighbors:
            thread = td.Thread(target=self.send_message,args=("I won",neighbor)) # Start Server
            thread.start()         
        thread2 = td.Thread(target=self.begin_trading,args=())
        thread2.start()                 
            
    # election_message: This method handles three types of messages:
    # 1) "election" : Upon receiving this message, peer will reply to the sender with "OK" message and if there are any higher peers, forwards the message and waits for OK messages, if it doesn't receives any then its the leader.
    # 2) "OK" : Drops out of the election, sets the flag didReceiveOK, which prevents it from further forwading the election message.
    # 3) "I won": Upon receiving this message, peer sets the leader details to the variable trader and starts the trading process. 
    def election_message(self,message,neighbor):
        if message == "election":
            # Fwd the election to higher peers, if available. Response here are Ok and I won.
            if self.didReceiveOK or self.didReceiveWon:
                thread = td.Thread(target=self.send_message,args=("OK",neighbor)) # Start Server
                thread.start()
            else:
                thread = td.Thread(target=self.send_message,args=("OK",neighbor)) # Start Server
                thread.start()
                peers = [x['peer_id'] for x in self.neighbors]
                peers = np.array(peers)
                x = len(peers[peers > self.peer_id])

                if x > 0:
                    self.flag_won_semaphore.acquire()
                    self.isElectionRunning = True # Set the flag
                    self.flag_won_semaphore.release()
                    self.didReceiveOK = False
                    for neighbor in self.neighbors:
                        if neighbor['peer_id'] > self.peer_id:
                            if self.trader != {} and neighbor['peer_id'] == self.trader['peer_id']:
                                pass
                            else:    
                                thread = td.Thread(target=self.send_message,args=("election",neighbor)) # Start Server
                                thread.start()
                    time.sleep(2.0)
                    
                    self.flag_won_semaphore.acquire()
                    if self.didReceiveOK == False and self.didReceiveWon == False: 
                        self.fwd_won_message() # Release of semaphore is done by that method.
                    else:
                        self.flag_won_semaphore.release()
                               
                elif x == 0:
                    self.flag_won_semaphore.acquire()
                    if self.didReceiveWon == False:
                        self.fwd_won_message()
                    else:
                        self.flag_won_semaphore.release()
                        
        elif message == 'OK':
            # Drop out and wait
            self.didReceiveOK = True
            
            
        elif message == 'I won':
            print("Peer ",self.peer_id,": Election Won Msg Received")
            #self.didReceiveOK = False
            self.flag_won_semaphore.acquire()
            self.didReceiveWon = True
            self.flag_won_semaphore.release()
            self.trader = neighbor
            time.sleep(5.0)
            thread2 = td.Thread(target=self.begin_trading,args=())
            thread2.start()
            # self.leader is neighbor, if  peer is a seller, he has to register his products with the trader.
    
    # start_election: This methods starts the election by forwading election message to the peers, if there are no higehr peers, then its the leader and sends the "I won" message to the peers.        
    def start_election(self):
        print ("Peer ",self.peer_id,": Started the election")
        self.flag_won_semaphore.acquire()
        self.isElectionRunning = True # Set the flag
        self.flag_won_semaphore.release()
        time.sleep(1)
        # Check number of peers higher than you.
        peers = [x['peer_id'] for x in self.neighbors]
        peers = np.array(peers)
        x = len(peers[peers > self.peer_id])
        if x > 0:
            self.didReceiveOK = False
            self.didReceiveWon = False
            for neighbor in self.neighbors:
                if neighbor['peer_id'] > self.peer_id:
                    #todo
                    # if self.trader != {} and neighbor['peer_id'] == self.trader['peer_id']: #  Don't send it to previous trader as he left the position.
                    #     pass
                    # else:   
                    thread = td.Thread(target=self.send_message,args=("election",neighbor)) # Start Server
                    thread.start()  
            time.sleep(2.0)
            self.flag_won_semaphore.acquire()
            if self.didReceiveOK == False and self.didReceiveWon == False:
               self.fwd_won_message()
            else:
                self.flag_won_semaphore.release()
        else: # No higher peers
            self.flag_won_semaphore.acquire()
            self.fwd_won_message() # Release of semaphore is in fwd_won_message
     
    # begin_trading : For a seller, through this method they register there product at the trader. For buyer, they start lookup process for the products needed, in this lab every lookup process is directed at the trader and he sells those goods on behalf of the sellers.            
    def begin_trading(self):
        time.sleep(2) # Delay so that al the election message are replied or election is dropped by peers other than the trader.
        # Reset the flags.
        self.isElectionRunning = False
        self.didReceiveWon = False
        self.didReceiveOK = False
        # If Seller, register the poducts.
        if self.db["Role"] == "Seller":
            connected,proxy = self.get_rpc(self.trader["host_addr"])
            p_n = None
            p_c = None
            for product_name, product_count in self.db['Inv'].items():
                if product_count > 0:
                    p_n= product_name
                    p_c = product_count
            seller_info = {'seller': {'peer_id':self.peer_id,'host_addr':self.host_addr},'product_name':p_n,'product_count':p_c} 
            if connected:
                proxy.register_products(seller_info)
        # If buyer, wait for 2 sec for seller to register products and then start buying.
        elif self.db["Role"] == "Buyer":
            time.sleep(1.0 + self.peer_id/10.0) # Allow sellers to register the products.
            while len(self.db['shop'])!= 0: 
                if self.isElectionRunning == True:
                    return # If election has started, then stop the process. (This process is restarted once a new leader is elected.)
                else:
                    item = self.db['shop'][0]
                    connected,proxy = self.get_rpc(self.trader["host_addr"])
                    if connected:
                        #increment buyer
                        self.clock_forward('Buyer',self.peer_id)
                        print ("Peer ",self.peer_id, ": Requesting ",item)
                        proxy.lookup(self.peer_id,self.host_addr,item)
                        time.sleep(1.0)
                    else: # Trader is Down.
                        # Re-Election
                        for neighbor in self.neighbors:
                            thread = td.Thread(target = self.send_restart_election_messages,args = (" ",neighbor))
                            thread.start() # Sending Neighbors reelection notification.
                        thread = td.Thread(target=self.start_election,args=())
                        thread.start()
        """ else:
            if os.path.isfile("seller_info.csv"): 
               trade_list = read_seller_log() 
            if os.path.isfile("transactions.csv"):
                unserved_request = get_unserved_requests()
                if unserved_request is None:
                    pass
                else:
                    pass """
                    #print(unserved_request)
                    #for i,j in unserved_request.items():
                    #    k,v = i,j
                    #self.lookup(v['buyer_id'],,v['product_name'])                          
    
    # register_products: Trader registers the seller goods.
    def register_products(self,seller_info): # Trader End.
        self.trade_list_semaphore.acquire()
        self.trade_list[str(seller_info['seller']['peer_id'])] = seller_info 
        self.trade_list_semaphore.release()

    # Check if all other buyers have already bought, If this is true then can buy else wait for others to buy first 
    def clockCheck(self,buyer_id):
        wanted_keys = [1,3] # All buyers
        only_buyer  = dict((k, self.clock[k]) for k in wanted_keys if k in self.clock)
        return any([only_buyer[buyer_id]-only_buyer[i]>1 for i in only_buyer])
    
    # lookup : Trader lookups the product that a buyer wants to buy and replies respective seller and buyer.     
    def lookup(self,buyer_id,host_addr,product_name):
        print(self.clock)
        #if buyer_id == 3:
        #    return
        #Check value of other peers and if difference greater than 1 then wait
        while(self.clockCheck(buyer_id)):
            print('Sleeping')
            time.sleep(2)
        #Get clock of buyer and increase it
        self.clock_forward('Buyer',buyer_id)
        
        seller_list = []
        for peer_id,seller_info in self.trade_list.items():
            if seller_info["product_name"] == product_name:
                #print "Product Found"
                seller_list.append(seller_info["seller"])
        if len(seller_list) > 0:
            # Log the request
            seller = seller_list[0]
            
            transaction_log = {str(self.clock[self.peer_id]) : {'product_name' : product_name, 'buyer_id' : buyer_id, 'seller_id':seller,'completed':False}}
            log_transaction('transactions.csv',transaction_log)
            connected,proxy = self.get_rpc(host_addr)
            
            self.trade_list_semaphore.acquire()
            
            self.trade_list[str(seller['peer_id'])]["product_count"]  = self.trade_list[str(seller['peer_id'])]["product_count"] -1     
            
            self.trade_list_semaphore.release()
            if connected: # Pass the message to buyer that transaction is succesful
                proxy.transaction(product_name,seller,buyer_id,self.trade_count)
            connected,proxy = self.get_rpc(seller["host_addr"])
            if connected:# Pass the message to seller that its product is sold
                proxy.transaction(product_name,seller,buyer_id,self.trade_count)
                
            # Relog the request as done ***Fix last arg as buyers clock**
            mark_transaction_complete('transactions.csv',transaction_log,str(0))

            
    # transaction : Seller just deducts the product count, Buyer prints the message.    
    def transaction(self, product_name, seller_id, buyer_id,trade_count): # Buyer & Seller
        if self.db["Role"] == "Buyer":
            print ( "Peer ", self.peer_id, " : Bought ",product_name, " from peer: ",seller_id["peer_id"])
            #self.db['shop'].remove(product_name)  
        elif self.db["Role"] == "Seller":
            self.db['Inv'][product_name] = self.db['Inv'][product_name] - 1
            #print "Sold ", product_name, " to peer: ",buyer_id["peer_id"]     
            if self.db['Inv'][product_name] == 0:
                # Pickup a random item and register that product with trader.
                product_list = ['Fish','Salt','Boar']
                x = random.randint(0, 2)
                self.db['Inv'][product_list[x]] = 3
                seller_info = {'seller': {'peer_id':self.peer_id,'host_addr':self.host_addr},'product_name':product_list[x],'product_count':3}
                
                connected,proxy = self.get_rpc(self.trader["host_addr"]) 
                if connected: 
                    proxy.register_products(seller_info)
db_load = {
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish"]}',
    2:'{"Role": "Seller","Inv":{"Fish":300},"shop":{}}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Salt","Boar","Fish"]}',
    4:'{"Role": "Seller","Inv":{"Fish":300,"Boar":0,"Salt":0},"shop":{}}',
    5:'{"Role": "Buyer","Inv":{},"shop":["Boar","Fish","Salt"]}',
    6:'{"Role": "Seller","Inv":{"Fish":300,"Boar":300,"Salt":3},"shop":{}}',
    7:'{"Role": "Seller","Inv":{"Fish":300,"Boar":300,"Salt":3},"shop":{}}'
}                   
if __name__ == "__main__":
    port = 10006
    host_ip = '127.0.0.1'
    totalPeers = int(sys.argv[1])
    for peerId in range(1,totalPeers+1):
        host_addr = host_ip + ":" + str(port+peerId)
        peer_id = peerId
        db = json.loads(db_load[peerId])
        num_peers = totalPeers
        
        # Computing Neigbors
        peer_ids = [x for x in range(1,num_peers+1)]
        host_ports = [(port + x) for x in range(1,num_peers+1)]
        host_addrs = [(host_ip + ':' + str(port)) for port in host_ports]
        neighbors = [{'peer_id':p,'host_addr':h} for p,h in zip(peer_ids,host_addrs)]
        neighbors.remove({'peer_id':peer_id,'host_addr':host_addr})
        
        #Declare a peer variable and start it.  
        peer_local = peer(host_addr,peer_id,neighbors,db,totalPeers)
        thread1 = td.Thread(target=peer_local.startServer,args=()) # Start Server
        thread1.start()    
        # Starting the election, lower peers.
        if peer_id <= 2:
            thread1 = td.Thread(target=peer_local.start_election,args=()) # Start Server
            thread1.start()