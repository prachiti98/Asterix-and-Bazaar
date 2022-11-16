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

class LamportClock:
    #: The clocks current value.
    value = 0

    def __init__(self, initial_value=0):
        self.value = initial_value

    def adjust(self, other):
        self.value = max(self.value, other)

    def forward(self):
        self.value += 1
        return self.value

# Peer
class peer:
    def __init__(self,host_addr,peer_id,neighbors,db):
        self.host_addr = host_addr
        self.peer_id = peer_id
        host_ip = '127.0.0.1'
        #host_addrs = [host_ip + ':10007',host_ip + ':10008',host_ip + ':10009',host_ip + ':10010',host_ip + ':10011',host_ip + ':10012',host_ip + ':10013']
        #peer_ids = [1,2,3,4,5,6,7]
        #self.neighbors = [{'peer_id':p,'host_addr':h} for p,h in zip(peer_ids,host_addrs)]
        #self.neighbors.remove({'peer_id':self.peer_id,'host_addr':self.host_addr})
        self.neighbors = neighbors
        self.db = db 
        self.trader = {} 
       
        # Shared Resources
        self.didReceiveOK = False # Flag
        self.didReceiveWon = False # Flag
        self.isElectionRunning = False # Flag
        self.trade_list = {}  #only for trader to save seller info
        
        # Lamport Clock.
        self.lamport_clock = LamportClock()
        
        # Trade Counter
        self.trade_count = 0
       
        # Semaphores 
        self.flag_won_semaphore = td.BoundedSemaphore(1) 
        self.trade_list_semaphore = td.BoundedSemaphore(1) #required only for trader
        self.clock_semaphore = td.BoundedSemaphore(1)       
        
   # Helper Method: Returns the proxy for specified address.
    def get_rpc(self,neighbor):
        a = xmlrpc.client.ServerProxy('http://' + str(neighbor) + '/')
            
        # Just in case the method is registered in the XmlRPC server
        return True, a
        
    # Starting Server    
    def startServer(self):
        # Start Server and register its functions.
        host_ip = '127.0.0.1'
        print(host_ip)
        server = AsyncXMLRPCServer((host_ip,int(self.host_addr.split(':')[1])),allow_none=True,logRequests=False)
        print(server)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.transaction,'transaction')
        server.register_function(self.election_message,'election_message')
        server.register_function(self.register_products,'register_products')
        server.register_function(self.adjust_buyer_clock,'adjust_buyer_clock')
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
        thread2 = td.Thread(target=peer_local.begin_trading,args=())
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
            thread2 = td.Thread(target=peer_local.begin_trading,args=())
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
        time.sleep(2) # Delay so that all the election message are replied or election is dropped by peers other than the trader.
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
                        self.clock_semaphore.acquire()
                        self.lamport_clock.forward()
                        request_ts = self.lamport_clock.value
                        #print "Buy Event",self.lamport_clock.value,self.peer_id
                        self.broadcast_lamport_clock()
                        self.clock_semaphore.release()
                        print ("Peer ",self.peer_id, ": Requesting ",item)
                        proxy.lookup({'peer_id':self.peer_id,'host_addr':self.host_addr},item,request_ts)
                        if self.lamport_clock.value % 6 == 0: # Once the 6th transaction is done, start new election to relieve the present leader.
                            for neighbor in self.neighbors:
                                thread = td.Thread(target = self.send_restart_election_messages,args = (" ",neighbor))
                                thread.start() # Sending Neighbors reelection notification.
                            thread = td.Thread(target=self.start_election,args=())
                            thread.start()                           
                        time.sleep(1.0)
                    else: # Trader is Down.
                        # Re-Election
                        for neighbor in self.neighbors:
                            thread = td.Thread(target = self.send_restart_election_messages,args = (" ",neighbor))
                            thread.start() # Sending Neighbors reelection notification.
                        thread = td.Thread(target=self.start_election,args=())
                        thread.start()
        else:
            if os.path.isfile("seller_info.csv"): 
               trade_list = read_seller_log() 
            if os.path.isfile("transactions.csv"):
                unserved_request = get_unserved_requests()
                if unserved_request is None:
                    pass
                else:
                    print(unserved_request)
                    for i,j in unserved_request.items():
                        k,v = i,j
                    self.lookup(v['buyer_id'],v['product_name'],int(k))                          
            
    # broadcast_lamport_clock : This method broadcasts a peer's clock to all the peers.
    def broadcast_lamport_clock(self):
        for neighbor in self.neighbors:
            thread = td.Thread(target=self.send_broadcast_message,args=("I won",neighbor)) # Start Server
            thread.start()
            
    def send_broadcast_message(self,_,neighbor): # Broadcast the clock.
        connected,proxy = self.get_rpc(neighbor['host_addr'])
        if connected:
            proxy.adjust_buyer_clock(self.lamport_clock.value) 
    
    # adjust_buyer_clock: Upon receiving this message, a peer adjusts its clock, only buyer and seller adjust there clock, but not the trader.(This is a design choice.)
    def adjust_buyer_clock(self, other):
        if self.db["Role"] == "Buyer" or self.db["Role"] != "Trader": # Trader should not adjust his clock until he recives the lookup request.
            self.clock_semaphore.acquire()
            self.lamport_clock.adjust(other) 
            self.clock_semaphore.release()
            #print  peer_id," ", self.lamport_clock.value
    
    # register_products: Trader registers the seller goods.
    def register_products(self,seller_info): # Trader End.
        self.trade_list_semaphore.acquire()
        self.trade_list[str(seller_info['seller']['peer_id'])] = seller_info 
        self.trade_list_semaphore.release()
    
    # lookup : Trader lookups the product that a buyer wants to buy and replies respective seller and buyer.     
    def lookup(self,buyer_id,product_name,buyer_clock):
        while self.lamport_clock.value - buyer_clock != -1:
            #print "Peer Waiting is ", buyer_id, buyer_clock
            time.sleep(2.0)
        
        self.lamport_clock.adjust(buyer_clock)
        #print "Trader's Clock",self.lamport_clock.value
        
        seller_list = []
        for peer_id,seller_info in self.trade_list.items():
            if seller_info["product_name"] == product_name:
                #print "Product Found"
                seller_list.append(seller_info["seller"])
        if len(seller_list) > 0:
            # Log the request
            seller = seller_list[0]
            
            transaction_log = {str(self.lamport_clock.value) : {'product_name' : product_name, 'buyer_id' : buyer_id, 'seller_id':seller,'completed':False}}
            log_transaction('transactions.csv',transaction_log)
            connected,proxy = self.get_rpc(buyer_id["host_addr"])
            
            self.trade_list_semaphore.acquire()
            
            self.trade_list[str(seller['peer_id'])]["product_count"]  = self.trade_list[str(seller['peer_id'])]["product_count"] -1     
            
            self.trade_list_semaphore.release()
            if connected: # Pass the message to buyer that transaction is succesful
                proxy.transaction(product_name,seller,buyer_id,self.trade_count)
            connected,proxy = self.get_rpc(seller["host_addr"])
            if connected:# Pass the message to seller that its product is sold
                proxy.transaction(product_name,seller,buyer_id,self.trade_count)
                
            # Relog the request as done
            mark_transaction_complete('transactions.csv',transaction_log,str(buyer_clock))
            # Soon there will be a change in trader, so save the seller list to a file.
            if buyer_clock == 6:
                seller_log(self.trade_list)
            
    # transaction : Seller just deducts the product count, Buyer prints the message.    
    def transaction(self, product_name, seller_id, buyer_id,trade_count): # Buyer & Seller
        if self.db["Role"] == "Buyer":
            print ( "Peer ", self.peer_id, " : Bought ",product_name, " from peer: ",seller_id["peer_id"])
            self.db['shop'].remove(product_name)  
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
                   
if __name__ == "__main__":
    host_ip = '127.0.0.1'
    host_addr = host_ip + ":" + sys.argv[2]
    peer_id = int(sys.argv[1])
    db = json.loads(sys.argv[3])
    num_peers = int(sys.argv[4])
    
    # Computing Neigbors
    peer_ids = [x for x in range(1,num_peers+1)]
    host_ports = [(10030 + x) for x in range(0,num_peers)]
    host_addrs = [(host_ip + ':' + str(port)) for port in host_ports]
    neighbors = [{'peer_id':p,'host_addr':h} for p,h in zip(peer_ids,host_addrs)]
    neighbors.remove({'peer_id':peer_id,'host_addr':host_addr})
    
    #Declare a peer variable and start it.  
    peer_local = peer(host_addr,peer_id,neighbors,db)
    thread1 = td.Thread(target=peer_local.startServer,args=()) # Start Server
    thread1.start()    
    # Starting the election, highest peer to not waste time
    if peer_id == num_peers:
        thread1 = td.Thread(target=peer_local.start_election,args=()) # Start Server
        thread1.start()