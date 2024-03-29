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
def logTransaction(filename,log):
    log = json.dumps(log)
    with open(filename,'a', newline='') as csvF:
        csvWriter = csv.writer(csvF,delimiter = ' ')
        csvWriter.writerow([log])
        
# Mark Transaction Complete        
def completeTransaction(filename,transaction,identifier):
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
def logSeller(tradeList):
    with open('sellerInfo.csv','w', newline='') as csvF:
        csvWriter = csv.writer(csvF,delimiter = ' ')
        for k,v in tradeList.items():
            log = json.dumps({k:v})
            csvWriter.writerow([log])  
            

class AsyncXMLRPCServer(socketserver.ThreadingMixIn,SimpleXMLRPCServer): pass

# Defining peer
class peer:
    def __init__(self,hostAddr,peerId,neighbors,db,totalPeers):
        self.hostAddr = hostAddr
        self.peerId = peerId
        self.neighbors = neighbors
        self.db = db 
        self.latency = 0
        self.requestCount = 0
        self.trader = {} 
        self.didReceiveOK = False # Flag
        self.didReceiveWon = False # Flag
        self.isElectionRunning = False # Flag
        self.tradeList = {} 
        self.clock  =  {i:0 for i in range(1,totalPeers+1)}
        self.tradeCount = 0
        self.balance = self.db["Balance"]
        self.wonLock = Lock() 
        self.tradeListLock = Lock()
        self.clockLock = Lock()    
        self.balanceLock = Lock()  
        self.tradeCountLock = Lock()
   # Gets the the proxy for specified address.
    def getRpc(self,neighbor):
        #Force the leader to fail. Test using test.py case #5
        #if self.requestCount == 2 and neighbor == '127.0.0.1:10034':
        #    print(self.trader["hostAddr"])
        #    print('Could not connected to trader')
        #    return False,'Failed!'
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

    #Increment clock by 1
    def tradeCountIncrease(self):    
        with self.tradeCountLock:    
            self.tradeCount+=1

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
            
    #Send election restart messages    
    def restartElectionMsg(self,_,neighbor):
        connected,proxy = self.getRpc(neighbor['hostAddr'])
        if connected:
            proxy.electionRestart() 
            
     #Send election message to a peer                
    def sendMsg(self,message,neighbor):
        connected,proxy = self.getRpc(neighbor['hostAddr'])
        if connected:
            proxy.electionMessage(message,{'peerId':self.peerId,'hostAddr':self.hostAddr})

    #Send the flags and send the "I won" message to peers
    def forwardWonMsg(self):
        with open('Peer_'+str(self.peerId)+".txt", "a") as f:
            f.write(" ".join(["Dear buyers and sellers, My ID is ",str(self.peerId), "and I am the new coordinator",'\n']))
        self.didReceiveWon = True
        self.trader = {'peerId':self.peerId,'hostAddr':self.hostAddr}
        self.db['Role'] = 'Trader'
        self.wonLock.release()
        for neighbor in self.neighbors:
            thread = td.Thread(target=self.sendMsg,args=("I won",neighbor)) 
            thread.start()         
        thread2 = td.Thread(target=self.beginTrading,args=())
        thread2.start()                 
            
    # electionMessage: This method handles three types of messages:
    # 1) "election" : Upon receiving this message, peer will reply to the sender with "OK" message and if there are any higher peers, forwards the message and waits for OK messages, if it doesn't receives any then its the leader.
    # 2) "OK" : Drops out of the election, sets the flag didReceiveOK, which prevents it from further forwading the election message.
    # 3) "I won": Upon receiving this message, peer sets the leader details to the variable trader and starts the trading process. 
    def electionMessage(self,message,neighbor):
        if message == "election":
            #Forward election msg to higher peers that are present. Get reposnes of 'ok' and 'I won'
            if self.didReceiveOK or self.didReceiveWon:
                thread = td.Thread(target=self.sendMsg,args=("OK",neighbor)) 
                thread.start()
            else:
                thread = td.Thread(target=self.sendMsg,args=("OK",neighbor)) 
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
                        # Release the lock 
                        self.forwardWonMsg() 
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
            with self.wonLock:
                self.didReceiveWon = True
            self.trader = neighbor
            time.sleep(5.0)
            thread2 = td.Thread(target=self.beginTrading,args=())
            thread2.start()
            # self.leader is neighbor, if  peer is a seller, he has to register his products with the trader.

    #Begins the election by forwarding the election message to the peers; if no higher peers are present, the leader takes over and sends the "I won" message to the peers.
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
            self.forwardWonMsg() # Release of lock is in forwardWonMsg
     
    # beginTrading : For a seller, through this method they register there product at the trader. For buyer, they start lookup process for the products needed, in this lab every lookup process is directed at the trader and he sells those goods on behalf of the sellers.            
    def beginTrading(self):
        # Sleep until all election messages have been responded to or the election has been dropped by peers other than the trader.
        time.sleep(2) 
        # Reset the flags.
        self.isElectionRunning = False
        self.didReceiveWon = False
        self.didReceiveOK = False
        # Seller registers products
        if self.db["Role"] == "Seller":
            connected,proxy = self.getRpc(self.trader["hostAddr"])
            for productName, productCount in self.db['Inv'].items():
                if productCount > 0:
                    sellerInfo = {'seller': {'peerId':self.peerId,'hostAddr':self.hostAddr,'productName':productName},'productName':productName,'productCount':productCount} 	
                if connected:
                    proxy.registerProducts(sellerInfo)
                # If trader is down in the beginning, then re-elect.
                else: 
                    with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                            f.write("Restarting election\n")
                    for neighbor in self.neighbors:
                        thread = td.Thread(target = self.restartElectionMsg,args = (" ",neighbor))
                        # Start re-election
                        thread.start() 
                    thread = td.Thread(target=self.StartElection,args=())
                    thread.start()
                time.sleep(1)
        # If you are a buyer, wait 2 seconds for the seller to register the products before you begin purchasing.
        elif self.db["Role"] == "Buyer":
            # Sellers register the products during this time.
            time.sleep(2) 
            while len(self.db['shop'])!= 0: 
                time.sleep(random.randint(1,5))
                if self.isElectionRunning == True:
                    # If election has started, then stop the process. (This process is restarted once a new leader is elected.)
                    return 
                else:
                    item = self.db['shop'][0]
                    connected,proxy = self.getRpc(self.trader["hostAddr"])
                    if connected:
                        #increment buyer
                        self.clockForward()
                        with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                            f.write(" ".join([str(datetime.datetime.now()),"Peer ",str(self.peerId), ": Requesting ",item,'\n']))
                        timeStart = datetime.datetime.now()
                        proxy.lookup(self.peerId,self.hostAddr,item,json.dumps(self.clock))
                        timeEnd = datetime.datetime.now()
                        self.calculateLatency(timeStart, timeEnd)
                    # Trader is Down.
                    else: 
                        with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                            f.write("Restarting election\n")
                        # Re-Election
                        for neighbor in self.neighbors:
                            thread = td.Thread(target = self.restartElectionMsg,args = (" ",neighbor))
                            thread.start() # Sending Neighbors reelection notification.
                        thread = td.Thread(target=self.startElection,args=())
                        thread.start()
                    time.sleep(1)
                     
    
    # registerProducts: Trader registers the seller goods.
    def registerProducts(self,sellerInfo): # Trader End.
        with self.tradeListLock:
            self.tradeList[str(sellerInfo['seller']['peerId'])+'_'+str(sellerInfo['seller']['productName'])] = sellerInfo 	
            logSeller(self.tradeList) #not sure

    # Check if all other buyers have already bought and informed by the trader that the item was successfully purchased
    # If all buyers have already bought and been informed by trader then allow the current buy request, else delay the buy request
    def clockCheck(self,buyer_id):
        buyer_keys = map(int,list(set([str(i) for i in range(1,totalPeers+1)])-set(list(i.split('_')[0] for i in list(self.tradeList.keys()))+list(str(self.trader['peerId'])))))  # All buyers
        only_buyer  = dict((k, self.clock[k]) for k in buyer_keys if k in self.clock)
        return any([only_buyer[buyer_id]-only_buyer[i]>2 for i in only_buyer])
    
    
    def printOnConsole(self, msg):
        with addLock:
            print(msg)

    #Trader lookups the product that a buyer wants to buy and replies respective seller and buyer.
    def lookup(self,buyer_id,hostAddr,productName,buyer_clock):
        self.tradeCountIncrease()
        buyer_clock = json.loads(buyer_clock)
        buyer_clock = {int(k):int(v) for k,v in buyer_clock.items()}
        tot_prod = 0
        for i in self.tradeList:
            if self.tradeList[i]['productName'] == productName:
                    tot_prod+=self.tradeList[i]['productCount']
        #When there's only few items left and they are requesting the same  then check the current buyer and all other buyers clock. If other buyers have lower cclock then make the current buyer sleep for 2 seconds.  
        #Buyer with lower clock then gets to buy  
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
        sellerList = []
        for peerId,sellerInfo in self.tradeList.items():
            if sellerInfo["productName"] == productName:
                #print "Product Found"
                sellerList.append(sellerInfo["seller"])
        if len(sellerList) > 0:
            # Log the request
            seller = sellerList[0]
            transactionLog = {str(self.tradeCount) : {'productName' : productName, 'buyer_id' : buyer_id, 'sellerId':seller,'completed':False}}
            logTransaction('transactions.csv',transactionLog)
            connected,proxy = self.getRpc(hostAddr)
            with self.tradeListLock:
                self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"]  = self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"] -1     	   
            if connected: # Pass the message to buyer that transaction is succesful
                proxy.transaction(productName,seller,buyer_id,self.tradeCount)
            connected,proxy = self.getRpc(seller["hostAddr"])
            # Pass the message to seller that its product is sold
            if connected:
                proxy.transaction(productName,seller,buyer_id,self.tradeCount)
            self.balance+=COMMISSION
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(self.peerId),"Trader's Current Balance:",str(self.balance),"\n"]))
            # Relog the request as done ***Fix last arg as buyers clock**
            completeTransaction('transactions.csv',transactionLog,str(self.tradeCount))
        else:
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(self.peerId),"Item is not present!","\n"]))


	# transaction : Seller just deducts the product count, Buyer prints the message.    	
    def transaction(self, productName, sellerId, buyer_id,tradeCount):
        if self.db["Role"] == "Buyer":	
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(datetime.datetime.now()),"Peer ", str(self.peerId), " : Bought ",productName, " from peer: ",str(sellerId["peerId"]),'\n']))
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
                #Send message from Trader to Buyer and adjust Buyer clock
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
            self.db['Inv'][productName] = self.db['Inv'][productName] - 1	
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(datetime.datetime.now()),str(self.peerId),"sold an item. Remaining items are:",str( self.db['Inv']),'\n']))	
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
            #Send message from Trader to Seller and adjust Seller clock
            self.clockAdjust(otherClock,self.trader['peerId'])
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join(['Trader->Buyer','Trader clock:',str(otherClock),'Seller clock:',str(self.clock),'\n']))
            if self.db['Inv'][productName] == 0:	
                # Refill the item with seller               	
                x = random.randint(1, 10)	
                with open('Peer_'+str(self.peerId)+".txt", "a") as f:	
                    f.write(" ".join([str(self.peerId),"restocking",str(productName),'by',str(x),'more','\n']))
                self.db['Inv'][productName] = x	
                sellerInfo = {'seller': {'peerId':self.peerId,'hostAddr':self.hostAddr,'productName':productName},'productName':productName,'productCount':x}	
                connected,proxy = self.getRpc(self.trader["hostAddr"]) 	
                if connected: 	
                    proxy.registerProducts(sellerInfo)	

        
db_load = {
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 1140}',
    2:'{"Role": "Seller","Inv":{"Fish":5},"shop":{},"Balance": 0}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 1140}',
    4:'{"Role": "Seller","Inv":{"Fish":5,"Boar":1,"Salt":2},"shop":{},"Balance": 0}',
    5:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"],"Balance": 1100}',
    6:'{"Role": "Seller","Inv":{"Fish":30,"Boar":30,"Salt":3},"shop":{},"Balance": 0}'
}                   
if __name__ == "__main__":
    port = 10030
    HostIp = '127.0.0.1'
    totalPeers = int(sys.argv[1])
    print("Marketplace is live! Check Peer_X.txt for logging!\n")
    try:
        os.remove('transactions.csv')
    except OSError:
        pass
    if totalPeers<3:
        print('Less than 3 peers passed!')
    else:
        buyerCnt = sum([1 if 'Buyer' in db_load[i] else 0 for i in db_load])
        sellerCnt = sum([1 if 'Seller' in db_load[i] else 0 for i in db_load])
        if buyerCnt<1 or sellerCnt<1:
            print('Enter atleast 1 buyer and seller!')
        else:
            for peerId in range(1,totalPeers+1):
                hostAddr = HostIp + ":" + str(port+peerId)
                peerId = peerId
                db = json.loads(db_load[peerId])
                num_peers = totalPeers
                
                # Creating the fully connected network
                peer_ids = [x for x in range(1,num_peers+1)]
                host_ports = [(port + x) for x in range(1,num_peers+1)]
                host_addrs = [(HostIp + ':' + str(port)) for port in host_ports]
                neighbors = [{'peerId':p,'hostAddr':h} for p,h in zip(peer_ids,host_addrs)]
                neighbors.remove({'peerId':peerId,'hostAddr':hostAddr})
                
                #Start the thread 
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