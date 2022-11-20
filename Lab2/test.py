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
import datetime

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
def seller_log(tradeList):
    with open('sellerInfo.csv','w', newline='') as csvF:
        csvWriter = csv.writer(csvF,delimiter = ' ')
        for k,v in tradeList.items():
            log = json.dumps({k:v})
            csvWriter.writerow([log])  
            
# Read the seller log           
def read_seller_log():
    with open('sellerInfo.csv','r', newline='') as csvF:
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
    def __init__(self,hostAddr,peerId,neighbors,db,totalPeers):
        self.hostAddr = hostAddr
        self.peerId = peerId
        self.neighbors = neighbors
        self.db = db 
        self.latency = 0
        self.requestCount = 0
        self.trader = {} 
        # Shared Resources
        self.didReceiveOK = False # Flag
        self.didReceiveWon = False # Flag
        self.isElectionRunning = False # Flag
        self.tradeList = {} 
        # Vector Clock.
        self.clock  =  {i:0 for i in range(1,totalPeers+1)}
        # Trade Counter
        self.tradeCount = 0
        self.balance = self.db["Balance"]
        # Semaphores 
        self.wonLock = Lock() 
        self.tradeListLock = Lock()
        self.clockLock = Lock()    
        self.balanceLock = Lock()  
        
   # Helper Method: Returns the proxy for specified address.
    def getRpc(self,neighbor):
        if self.requestCount == 20 and self.peerId == 6:
            return False,'Failed!'
        a = xmlrpc.client.ServerProxy('http://' + str(neighbor) + '/')
        return True, a

    def calculateLatency(self, timeStart, timeStop):
        self.latency += (timeStop - timeStart).total_seconds()
        self.requestCount += 1
        if self.requestCount % 1000 == 0:
            self.printOnConsole('Average latency of peer '+str(self.peerId)+': '+str(self.latency / self.requestCount)+' (sec/req)')

    #Increment clock by 1
    def clockForward(self):    
        with self.clockLock:    
            self.clock[self.peerId]+=1
        return json.dumps(self.clock)

    def clockAdjust(self,sendingClock,sender_id):
        #Element wise max and max of buyer and its own clock 
        with self.clockLock:
            self.clock = {key:max(value,sendingClock[key]) for key, value in self.clock.items()}
            #Increment by 1 cause reciving clock max(a,b)+1
            self.clock[self.peerId]+=1
        
    # Starting Server    
    def startServer(self):
        # Start Server and register its functions.
        HostIp = '127.0.0.1'
        server = AsyncXMLRPCServer((HostIp,int(self.hostAddr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.transaction,'transaction')
        server.register_function(self.electionMessage,'electionMessage')
        server.register_function(self.registerProducts,'registerProducts')
        server.register_function(self.electionRestart,'electionRestart')
        server.register_function(self.clockForward,'clockForward')
        server.register_function(self.clockAdjust,'clockAdjust')
        server.serve_forever()
   
    # electionRestart : Once a peer recieves this messages, it indicates that a new election will be started.
    # Subsequent action is to set election running flag up.
    # This flag indicates to buyer who wants to buy, to wait till the election to restart the buying process.
    def electionRestart(self):
        with self.wonLock:
            self.isElectionRunning = True
            if self.db['Role'] == "Trader": 
                if len(self.db['shop'])!= 0:
                    self.db['Role'] = "Buyer"
                else:
                    self.db['Role'] = "Seller"
            
    # Helper method : To send election restart messages to a peer on a new thread.      
    def restartElectionMsg(self,_,neighbor):
        connected,proxy = self.getRpc(neighbor['hostAddr'])
        if connected:
            proxy.electionRestart() 
            
     # Helper method : To send election message to a peer on a new thread.                
    def sendMsg(self,message,neighbor):
        connected,proxy = self.getRpc(neighbor['hostAddr'])
        if connected:
            proxy.electionMessage(message,{'peerId':self.peerId,'hostAddr':self.hostAddr})

    # Helper method : To send the flags and send the "I won" message to peers.
    def forwardWonMsg(self):
        with open('Peer_'+str(self.peerId)+".txt", "a") as f:
            f.write(" ".join(["Dear buyers and sellers, My ID is ",str(self.peerId), "and I am the new coordinator",'\n']))
        self.didReceiveWon = True
        self.trader = {'peerId':self.peerId,'hostAddr':self.hostAddr}
        self.db['Role'] = 'Trader'
        self.wonLock.release()
        for neighbor in self.neighbors:
            thread = td.Thread(target=self.sendMsg,args=("I won",neighbor)) # Start Server
            thread.start()         
        thread2 = td.Thread(target=self.beginTrading,args=())
        thread2.start()                 
            
    # electionMessage: This method handles three types of messages:
    # 1) "election" : Upon receiving this message, peer will reply to the sender with "OK" message and if there are any higher peers, forwards the message and waits for OK messages, if it doesn't receives any then its the leader.
    # 2) "OK" : Drops out of the election, sets the flag didReceiveOK, which prevents it from further forwading the election message.
    # 3) "I won": Upon receiving this message, peer sets the leader details to the variable trader and starts the trading process. 
    def electionMessage(self,message,neighbor):
        if message == "election":
            # Fwd the election to higher peers, if available. Response here are Ok and I won.
            if self.didReceiveOK or self.didReceiveWon:
                thread = td.Thread(target=self.sendMsg,args=("OK",neighbor)) # Start Server
                thread.start()
            else:
                thread = td.Thread(target=self.sendMsg,args=("OK",neighbor)) # Start Server
                thread.start()
                peers = [x['peerId'] for x in self.neighbors]
                peers = np.array(peers)
                x = len(peers[peers > self.peerId])
                if x > 0:
                    with self.wonLock:
                        self.isElectionRunning = True # Set the flag
                    self.didReceiveOK = False
                    for neighbor in self.neighbors:
                        if neighbor['peerId'] > self.peerId:
                            if self.trader != {} and neighbor['peerId'] == self.trader['peerId']:
                                pass
                            else:    
                                thread = td.Thread(target=self.sendMsg,args=("election",neighbor)) # Start Server
                                thread.start()
                    time.sleep(2.0)
                    self.wonLock.acquire()
                    if self.didReceiveOK == False and self.didReceiveWon == False: 
                        self.forwardWonMsg() # Release of semaphore is done by that method.
                    else:
                        self.wonLock.release()      
                elif x == 0:
                    self.wonLock.acquire()
                    if self.didReceiveWon == False:
                        self.forwardWonMsg()
                    else:
                        self.wonLock.release()       
        elif message == 'OK':
            # Drop out and wait
            self.didReceiveOK = True
        elif message == 'I won':
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join(["Peer ",str(self.peerId),": Election Won Msg Received",'\n']))
            #self.didReceiveOK = False
            with self.wonLock:
                self.didReceiveWon = True
            self.trader = neighbor
            time.sleep(5.0)
            thread2 = td.Thread(target=self.beginTrading,args=())
            thread2.start()
            # self.leader is neighbor, if  peer is a seller, he has to register his products with the trader.
    
    # startElection: This methods starts the election by forwading election message to the peers, if there are no higehr peers, then its the leader and sends the "I won" message to the peers.        
    def startElection(self):
        with open('Peer_'+str(self.peerId)+".txt", "a") as f:
            f.write(" ".join(["Peer ",str(self.peerId),": Started the election",'\n']))
        with self.wonLock:
            self.isElectionRunning = True # Set the flag
        time.sleep(1)
        # Check number of peers higher than you.
        peers = [x['peerId'] for x in self.neighbors]
        peers = np.array(peers)
        x = len(peers[peers > self.peerId])
        if x > 0:
            self.didReceiveOK = False
            self.didReceiveWon = False
            for neighbor in self.neighbors:
                if neighbor['peerId'] > self.peerId:
                    #todo
                    # if self.trader != {} and neighbor['peerId'] == self.trader['peerId']: #  Don't send it to previous trader as he left the position.
                    #     pass
                    # else:   
                    thread = td.Thread(target=self.sendMsg,args=("election",neighbor)) # Start Server
                    thread.start()  
            time.sleep(2.0)
            self.wonLock.acquire()
            if self.didReceiveOK == False and self.didReceiveWon == False:
               self.forwardWonMsg()
            else:
                self.wonLock.release()
        else: # No higher peers
            self.wonLock.acquire()
            self.forwardWonMsg() # Release of semaphore is in forwardWonMsg
     
    # beginTrading : For a seller, through this method they register there product at the trader. For buyer, they start lookup process for the products needed, in this lab every lookup process is directed at the trader and he sells those goods on behalf of the sellers.            
    def beginTrading(self):
        time.sleep(2) # Delay so that al the election message are replied or election is dropped by peers other than the trader.
        # Reset the flags.
        self.isElectionRunning = False
        self.didReceiveWon = False
        self.didReceiveOK = False
        # If Seller, register the poducts.
        if self.db["Role"] == "Seller":
            connected,proxy = self.getRpc(self.trader["hostAddr"])
            for productName, productCount in self.db['Inv'].items():
                if productCount > 0:
                    sellerInfo = {'seller': {'peerId':self.peerId,'hostAddr':self.hostAddr,'productName':productName},'productName':productName,'productCount':productCount} 	
                if connected:
                    proxy.registerProducts(sellerInfo)
        # If buyer, wait for 2 sec for seller to register products and then start buying.
        elif self.db["Role"] == "Buyer":
            time.sleep(2) # Allow sellers to register the products.
            while len(self.db['shop'])!= 0: 
                time.sleep(random.randint(1,5))
                if self.isElectionRunning == True:
                    return # If election has started, then stop the process. (This process is restarted once a new leader is elected.)
                else:
                    item = self.db['shop'][0]
                    connected,proxy = self.getRpc(self.trader["hostAddr"])
                    if connected:
                        #increment buyer
                        self.clockForward()
                        with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                            f.write(" ".join(["Peer ",str(self.peerId), ": Requesting ",item,'\n']))
                        timeStart = datetime.datetime.now()
                        proxy.lookup(self.peerId,self.hostAddr,item,json.dumps(self.clock))
                        timeEnd = datetime.datetime.now()
                        self.calculateLatency(timeStart, timeEnd)
                    else: # Trader is Down.
                        # Re-Election
                        for neighbor in self.neighbors:
                            thread = td.Thread(target = self.restartElectionMsg,args = (" ",neighbor))
                            thread.start() # Sending Neighbors reelection notification.
                        thread = td.Thread(target=self.startElection,args=())
                        thread.start()
                    time.sleep(1)
        """ else:
            if os.path.isfile("sellerInfo.csv"): 
               self.tradeList = read_seller_log() 
            if os.path.isfile("transactions.csv"):
                unserved_request = get_unserved_requests()
                if unserved_request is None:
                    pass
                else:
                    pass """
                    #print(unserved_request)
                    #for i,j in unserved_request.items():
                    #    k,v = i,j
                    #self.lookup(v['buyer_id'],,v['productName'])                          
    
    # registerProducts: Trader registers the seller goods.
    def registerProducts(self,sellerInfo): # Trader End.
        with self.tradeListLock:
            self.tradeList[str(sellerInfo['seller']['peerId'])+'_'+str(sellerInfo['seller']['productName'])] = sellerInfo 	
            seller_log(self.tradeList) #not sure

    # Check if all other buyers have already bought, If this is true then can buy else wait for others to buy first 
    def clockCheck(self,buyer_id):
        buyer_keys = map(int,list(set([str(i) for i in range(1,totalPeers+1)])-set(list(i.split('_')[0] for i in list(self.tradeList.keys()))+list(str(self.trader['peerId'])))))  # All buyers
        only_buyer  = dict((k, self.clock[k]) for k in buyer_keys if k in self.clock)
        return any([only_buyer[buyer_id]-only_buyer[i]>2 for i in only_buyer])
    
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
    def lookup(self,buyer_id,hostAddr,productName,buyer_clock):
        buyer_clock = json.loads(buyer_clock)
        buyer_clock = {int(k):int(v) for k,v in buyer_clock.items()}
        tot_prod = 0
        for i in self.tradeList:
            if self.tradeList[i]['productName'] == productName:
                    tot_prod+=self.tradeList[i]['productCount']
        if tot_prod<10:
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write('too few'+'\n')
            self.clockAdjust(buyer_clock,buyer_id)   
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join(['Buyer->Trader after recieve','Trader clock:',str(self.clock),'Buyer clock:',str(buyer_clock),'\n']))  
            while self.clockCheck(buyer_id):
                with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                    f.write(" ".join([str(buyer_id),'Waiting for other','\n']))
                time.sleep(2)
        else:
            self.clockAdjust(buyer_clock,buyer_id)   
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join(['Buyer->Trader after recieve','Trader clock:',str(self.clock),'Buyer clock:',str(buyer_clock),'\n']))  
        seller_list = []
        for peerId,sellerInfo in self.tradeList.items():
            if sellerInfo["productName"] == productName:
                #print "Product Found"
                seller_list.append(sellerInfo["seller"])
        if len(seller_list) > 0:
            # Log the request
            seller = seller_list[0]
            transaction_log = {str(self.clock[self.peerId]) : {'productName' : productName, 'buyer_id' : buyer_id, 'sellerId':seller,'completed':False}}
            log_transaction('transactions.csv',transaction_log)
            connected,proxy = self.getRpc(hostAddr)
            with self.tradeListLock:
                self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"]  = self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"] -1     	   
            if connected: # Pass the message to buyer that transaction is succesful
                proxy.transaction(productName,seller,buyer_id,self.tradeCount)
            connected,proxy = self.getRpc(seller["hostAddr"])
            if connected:# Pass the message to seller that its product is sold
                proxy.transaction(productName,seller,buyer_id,self.tradeCount)
            self.balance+=COMMISSION
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(self.peerId),"Trader's Current Balance:",str(self.balance),"\n"]))
            # Relog the request as done ***Fix last arg as buyers clock**
            mark_transaction_complete('transactions.csv',transaction_log,str(0))
        else:
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join(["BuyerId:",str(buyer_id),"requesting item is not available!","\n"]))


        
	    # transaction : Seller just deducts the product count, Buyer prints the message.    	
    def transaction(self, productName, sellerId, buyer_id,tradeCount): # Buyer & Seller	
        if self.db["Role"] == "Buyer":	
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join(["Peer ", str(self.peerId), " : Bought ",productName, " from peer: ",str(sellerId["peerId"]),'\n']))
            if productName in self.db['shop']:	
                self.db['shop'].remove(productName)	
                with self.balanceLock:
                    if productName == "Boar":	
                        self.balance = self.balance - COST_BOAR	
                    elif productName == "Fish":	
                        self.balance = self.balance - COST_FISH	
                    elif productName == "Salt":	
                        self.balance = self.balance - COST_SALT	
                with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                    f.write(" ".join([str(self.peerId),"Current Balance:",str(self.balance),'\n']))	
                connected,proxy = self.getRpc(self.trader["hostAddr"]) 
                #Increase traders clock
                otherClock = json.loads(proxy.clockForward())
                otherClock = {int(k):int(v) for k,v in otherClock.items()}
                #Have to pass traders clock here not copy
                self.clockAdjust(otherClock,self.trader['peerId'])
                with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                    f.write(" ".join(['Trader->Buyer','Trader clock:',str(otherClock),'Buyer clock:',str(self.clock),'\n']))
            if len(self.db['shop']) == 0:	
                #print("No products with buyer",self.peerId)	
                productList = ["Fish","Salt","Boar"]	
                x = random.randint(0, 2)	
                self.db['shop'].append(productList[x])	
                with self.balanceLock:	
                    if productList[x] == "Boar":	
                        self.balance +=COST_BOAR	
                    elif productList[x] == "Fish":	
                        self.balance +=COST_FISH	
                    elif productList[x] == "Salt":	
                        self.balance += COST_SALT 
                with open('Peer_'+str(self.peerId)+".txt", "a") as f:	
                    f.write(" ".join([str(self.peerId),"Current Balance:",str(self.balance),'\n']))
        elif self.db["Role"] == "Seller":	
            #todo - remove the count requested by buyer	
            self.db['Inv'][productName] = self.db['Inv'][productName] - 1	
            with self.balanceLock:
                if productName == "Boar":	
                    self.balance += (COST_BOAR - COMMISSION)	
                elif productName == "Fish":	
                    self.balance += (COST_FISH - COMMISSION)	
                elif productName == "Salt":	
                    self.balance += (COST_SALT - COMMISSION)	
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(self.peerId),"Current Balance:",str(self.balance),'\n']))	
            connected,proxy = self.getRpc(self.trader["hostAddr"]) 
            #Increase traders clock
            otherClock = json.loads(proxy.clockForward())
            otherClock = {int(k):int(v) for k,v in otherClock.items()}
            #Have to pass traders clock here not copy
            self.clockAdjust(otherClock,self.trader['peerId'])
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join(['Trader->Buyer','Trader clock:',str(otherClock),'Seller clock:',str(self.clock),'\n']))
            if self.db['Inv'][productName] == 0:	
                # Refill the item with seller               	
                x = random.randint(1, 10)	
                self.db['Inv'][productName] = x	
                sellerInfo = {'seller': {'peerId':self.peerId,'hostAddr':self.hostAddr,'productName':productName},'productName':productName,'productCount':x}	
                connected,proxy = self.getRpc(self.trader["hostAddr"]) 	
                if connected: 	
                    proxy.registerProducts(sellerInfo)	

        
testcases = {1:{
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 1404}'
},
2:{
    1:'{"Role": "Seller","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 1404}',
    2:'{"Role": "Seller","Inv":{"Fish":15},"shop":{},"Balance": 0}',
    3:'{"Role": "Seller","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 404}',
},
3:{
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 1404}',
    2:'{"Role": "Buyer","Inv":{"Fish":15},"shop":{},"Balance": 0}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 404}',
},
4:{
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 1404}',
    2:'{"Role": "Seller","Inv":{"Fish":15},"shop":{},"Balance": 0}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 404}',
    4:'{"Role": "Seller","Inv":{"Fish":15},"shop":{},"Balance": 0}',
    5:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 1440}',
    6:'{"Role": "Seller","Inv":{"Fish":30,"Boar":30,"Salt":3},"shop":{},"Balance": 0}'
},
6:{
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 140}',
    2:'{"Role": "Seller","Inv":{"Fish":5},"shop":{},"Balance": 0}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 40}',
    4:'{"Role": "Seller","Inv":{"Fish":5,"Boar":1,"Salt":2},"shop":{},"Balance": 0}',
    5:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 0}',
    6:'{"Role": "Seller","Inv":{"Fish":30,"Boar":30,"Salt":3},"shop":{},"Balance": 0}'
},
7:{
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 140}',
    2:'{"Role": "Seller","Inv":{"Fish":5},"shop":{},"Balance": 0}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 40}',
    4:'{"Role": "Seller","Inv":{"Fish":5,"Boar":1,"Salt":2},"shop":{},"Balance": 0}',
    5:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 0}',
    6:'{"Role": "Seller","Inv":{"Fish":30,"Boar":30,"Salt":3},"shop":{},"Balance": 0}'
},
8:{
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 140}',
    2:'{"Role": "Seller","Inv":{"Fish":5},"shop":{},"Balance": 0}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Boar"],"Balance": 40}',
    4:'{"Role": "Seller","Inv":{"Fish":5,"Boar":1,"Salt":2},"shop":{},"Balance": 0}',
}
}  
                   
if __name__ == "__main__":
    port = 10030
    HostIp = '127.0.0.1'
    testCase = int(sys.argv[1])
    totalPeers = len(testcases[testCase].values())
    print("Marketplace is live! Check Peer_X.txt for logging!\n")
    if totalPeers<3:
        print('Less than 3 peers passed!')
    else:
        buyerCnt = sum([1 if 'Buyer' in testcases[testCase][i] else 0 for i in testcases[testCase]])
        sellerCnt = sum([1 if 'Seller' in testcases[testCase][i] else 0 for i in testcases[testCase]])
        if buyerCnt<1 or sellerCnt<1:
            print('Enter atleast 1 buyer and seller!')
        else:
            for peerId in range(1,totalPeers+1):
                hostAddr = HostIp + ":" + str(port+peerId)
                peerId = peerId
                db = json.loads(testcases[testCase][peerId])
                num_peers = totalPeers
                
                # Computing Neigbors
                peer_ids = [x for x in range(1,num_peers+1)]
                host_ports = [(port + x) for x in range(1,num_peers+1)]
                host_addrs = [(HostIp + ':' + str(port)) for port in host_ports]
                neighbors = [{'peerId':p,'hostAddr':h} for p,h in zip(peer_ids,host_addrs)]
                neighbors.remove({'peerId':peerId,'hostAddr':hostAddr})
                
                #Declare a peer variable and start it.  
                peer_local = peer(hostAddr,peerId,neighbors,db,totalPeers)
                thread1 = td.Thread(target=peer_local.startServer,args=()) # Start Server
                thread1.start()    
                # Starting the election, lower peers.
                try:
                    os.remove('Peer'+'_'+str(peerId)+'.txt')
                except OSError:
                    pass
                if peerId <= 2:
                    thread1 = td.Thread(target=peer_local.startElection,args=()) # Start Server
                    thread1.start()