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
from threading import Lock

#What are local events? (Only buy)
#When A sends again check trader clock if other buyers have completed just as many buys or else wait till they do

#all amounts are in USD
COST_BOAR = 10
COST_SALT = 5
COST_FISH = 20
COMMISSION = 2

addLock = Lock()

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

        self.balance = self.db["Balance"]
       
        # Semaphores 
        self.flag_won_semaphore = Lock() 
        self.trade_list_semaphore = Lock()
        self.clock_semaphore = Lock()    
        self.balance_semaphore = td.BoundedSemaphore(1) 
        
   # Helper Method: Returns the proxy for specified address.
    def get_rpc(self,neighbor):
        a = xmlrpc.client.ServerProxy('http://' + str(neighbor) + '/')
            
        # Just in case the method is registered in the XmlRPC server
        return True, a

    #From buyer to trader 
    #Trader to buyer
    def clock_forward(self):        
        self.clock_semaphore.acquire()
        self.clock[self.peer_id]+=1
        self.clock_semaphore.release()
        return json.dumps(self.clock)

    def clock_adjust(self,sending_clock,sender_id):
        #Element wise max and max of buyer and its own clock 
        self.clock_semaphore.acquire()
        self.clock = {key:max(value,sending_clock[key]) for key, value in self.clock.items()}
        #Increment by 1 cause reciving clock max(a,b)+1
        self.clock[self.peer_id]+=1
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
        server.register_function(self.clock_forward,'clock_forward')
        server.register_function(self.clock_adjust,'clock_adjust')
        server.serve_forever()
   
    # election_restart_message : Once a peer recieves this messages, it indicates that a new election will be started.
    # Subsequent action is to set election running flag up.
    # This flag indicates to buyer who wants to buy, to wait till the election to restart the buying process.
    def election_restart_message(self):
        with self.flag_won_semaphore:
            self.isElectionRunning = True
            if self.db['Role'] == "Trader": 
                if len(self.db['shop'])!= 0:
                    self.db['Role'] = "Buyer"
                else:
                    self.db['Role'] = "Seller"
            
        
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
        self.printOnConsole(" ".join(["Dear buyers and sellers, My ID is ",str(self.peer_id), "and I am the new coordinator"]))
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
                    with self.flag_won_semaphore:
                        self.isElectionRunning = True # Set the flag
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
            self.printOnConsole(" ".join(["Peer ",str(self.peer_id),": Election Won Msg Received"]))
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
        self.printOnConsole (" ".join(["Peer ",str(self.peer_id),": Started the election"]))
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
            for product_name, product_count in self.db['Inv'].items():
                if product_count > 0:
                    seller_info = {'seller': {'peer_id':self.peer_id,'host_addr':self.host_addr,'product_name':product_name},'product_name':product_name,'product_count':product_count} 	
                if connected:
                    proxy.register_products(seller_info)
        # If buyer, wait for 2 sec for seller to register products and then start buying.
        elif self.db["Role"] == "Buyer":
            time.sleep(1.0 + self.peer_id/10.0) # Allow sellers to register the products.
            while len(self.db['shop'])!= 0: 
                time.sleep(random.randint(1,5))
                if self.isElectionRunning == True:
                    return # If election has started, then stop the process. (This process is restarted once a new leader is elected.)
                else:
                    item = self.db['shop'][0]
                    connected,proxy = self.get_rpc(self.trader["host_addr"])
                    if connected:
                        #increment buyer
                        self.clock_forward()
                        self.printOnConsole (" ".join(["Peer ",str(self.peer_id), ": Requesting ",item]))
                        proxy.lookup(self.peer_id,self.host_addr,item,json.dumps(self.clock))
                        time.sleep(1.0)
                    else: # Trader is Down.
                        # Re-Election
                        for neighbor in self.neighbors:
                            thread = td.Thread(target = self.send_restart_election_messages,args = (" ",neighbor))
                            thread.start() # Sending Neighbors reelection notification.
                        thread = td.Thread(target=self.start_election,args=())
                        thread.start()
                    time.sleep(1)
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
        self.trade_list[str(seller_info['seller']['peer_id'])+'_'+str(seller_info['seller']['product_name'])] = seller_info 	
        seller_log(self.trade_list) #not sure
        self.trade_list_semaphore.release()

    # Check if all other buyers have already bought, If this is true then can buy else wait for others to buy first 
    def clockCheck(self,buyer_id):
        buyer_keys = map(int,list(set([str(i) for i in range(1,totalPeers+1)])-set(list(i.split('_')[0] for i in list(self.trade_list.keys()))+list(str(self.trader['peer_id'])))))  # All buyers
        #print(buyer_keys)
        only_buyer  = dict((k, self.clock[k]) for k in buyer_keys if k in self.clock)
        #only_buyer[buyer_id]-=1
        return any([only_buyer[buyer_id]-only_buyer[i]>1 for i in only_buyer])
    
    # lookup : Trader lookups the product that a buyer wants to buy and replies respective seller and buyer.

    def printOnConsole(self, msg):
        with addLock:
            print(msg)
    #Trader keeps track of stuff in trade list. 
    # Check if item in trade list is <3 quantity
    # Buyer with lower clock then gets to buy 

    #New Changes - 
    #Adjust trade clock to max buyer+1 and buyer clock to buyer. 
    #When there's only few items left then check the current buyer and all other buyers clock. 
    #If other buyers have lower cclock then make the current buyer sleep for 5 seconds.   
    def lookup(self,buyer_id,host_addr,product_name,buyer_clock):

        buyer_clock = json.loads(buyer_clock)
        buyer_clock = {int(k):int(v) for k,v in buyer_clock.items()}

        tot_prod = 0
        for i in self.trade_list:
            if self.trade_list[i]['product_name'] == product_name:
                    tot_prod+=self.trade_list[i]['product_count']

        if tot_prod<10:
            self.printOnConsole('Too few')
            self.clock_adjust(buyer_clock,buyer_id)   
            self.printOnConsole(" ".join(['Buyer->Trader after recieve','Trader clock:',str(self.clock),'Buyer clock:',str(buyer_clock)]))  
            while self.clockCheck(buyer_id):
                self.printOnConsole(" ".join([str(buyer_id),'Waiting for other']))
                time.sleep(2)
        else:
            self.clock_adjust(buyer_clock,buyer_id)   
            self.printOnConsole(" ".join(['Buyer->Trader after recieve','Trader clock:',str(self.clock),'Buyer clock:',str(buyer_clock)]))  

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
            self.trade_list[str(seller['peer_id'])+'_'+str(seller['product_name'])]["product_count"]  = self.trade_list[str(seller['peer_id'])+'_'+str(seller['product_name'])]["product_count"] -1     	   
            self.trade_list_semaphore.release()
            if connected: # Pass the message to buyer that transaction is succesful
                proxy.transaction(product_name,seller,buyer_id,self.trade_count)
            connected,proxy = self.get_rpc(seller["host_addr"])
            if connected:# Pass the message to seller that its product is sold
                proxy.transaction(product_name,seller,buyer_id,self.trade_count)
            #print(self.peer_id,"Current Balance:",self.balance)
                
            # Relog the request as done ***Fix last arg as buyers clock**
            mark_transaction_complete('transactions.csv',transaction_log,str(0))

        
	    # transaction : Seller just deducts the product count, Buyer prints the message.    	
    def transaction(self, product_name, seller_id, buyer_id,trade_count): # Buyer & Seller	
        if self.db["Role"] == "Buyer":	
            self.printOnConsole (" ".join(["Peer ", str(self.peer_id), " : Bought ",product_name, " from peer: ",str(seller_id["peer_id"])]))
            if product_name in self.db['shop']:	
                self.db['shop'].remove(product_name)	
                	
                self.balance_semaphore.acquire()	
                if product_name == "Boar":	
                    self.balance = self.balance - COST_BOAR	
                elif product_name == "Fish":	
                    self.balance = self.balance - COST_FISH	
                elif product_name == "Salt":	
                    self.balance = self.balance - COST_SALT	
                self.balance_semaphore.release()	
                #print(self.peer_id,"Current Balance:",self.balance)	
            if len(self.db['shop']) == 0:	
                #print("No products with buyer",self.peer_id)	
                product_list = ["Fish","Salt","Boar"]	
                x = random.randint(0, 0)	
                self.db['shop'].append(product_list[x])	
                self.balance_semaphore.acquire()	
                if product_list[x] == "Boar":	
                    self.balance +=COST_BOAR	
                elif product_list[x] == "Fish":	
                    self.balance +=COST_FISH	
                elif product_list[x] == "Salt":	
                    self.balance += COST_SALT 	
                self.balance_semaphore.release()   	
                #print(self.peer_id,"Started buying:",product_list[x])	
                #print(self.peer_id,"Current Balance:",self.balance)	
                thread2 = td.Thread(target=self.begin_trading,args=())	
                thread2.start()	
        elif self.db["Role"] == "Seller":	
            #todo - remove the count requested by buyer	
            self.db['Inv'][product_name] = self.db['Inv'][product_name] - 1	
            self.balance_semaphore.acquire()	
            if product_name == "Boar":	
                self.balance += (COST_BOAR - COMMISSION)	
            elif product_name == "Fish":	
                self.balance += (COST_FISH - COMMISSION)	
            elif product_name == "Salt":	
                self.balance += (COST_SALT - COMMISSION)	
            self.balance_semaphore.release() 	
            #print(self.peer_id,"Current Balance:",self.balance)	
            #print "Sold ", product_name, " to peer: ",buyer_id["peer_id"]     	
            if self.db['Inv'][product_name] == 0:	
                	
                # Refill the item with seller               	
                x = random.randint(1, 10)	
                self.db['Inv'][product_name] = x	
                seller_info = {'seller': {'peer_id':self.peer_id,'host_addr':self.host_addr,'product_name':product_name},'product_name':product_name,'product_count':x}	
                	
                connected,proxy = self.get_rpc(self.trader["host_addr"]) 	
                if connected: 	
                    proxy.register_products(seller_info)	

        
db_load = {
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 20}',
    2:'{"Role": "Seller","Inv":{"Fish":5},"shop":{},"Balance": 0}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 15}',
    4:'{"Role": "Seller","Inv":{"Fish":5,"Boar":1,"Salt":2},"shop":{},"Balance": 0}',
    5:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 0}',
    6:'{"Role": "Seller","Inv":{"Fish":30,"Boar":30,"Salt":3},"shop":{},"Balance": 0}'
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