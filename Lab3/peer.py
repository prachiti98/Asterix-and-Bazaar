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
from collections import deque
from collections import defaultdict

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

#Defining DB Server
class database:
    def __init__(self,hostAddr,peerId,neighbors,db):
        self.hostAddr = hostAddr
        self.peerId = peerId
        self.neighbors = neighbors
        self.db = db 
        self.latency = 0
        self.requestCount = 0
        #Storing data here for now. Can shift to file.
        self.tradeList = defaultdict(int) 
        self.tradeCount = 0
        self.requestQueue = deque()
        
        # Starting Server    
    def startServer(self):
        # Start Server and register its functions.
        HostIp = '127.0.0.1'
        server = AsyncXMLRPCServer((HostIp,int(self.hostAddr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.addProduct,'addProduct')
        server.register_function(self.removeProduct,'removeProduct')
        server.serve_forever()
        self.processRequests()

    def addProduct(self,product,quantity):
        self.tradeList[product]+=quantity
        return

    def removeProduct(self,product,quantity):
        self.tradeList[product]+=quantity
        return

    def addProductRequest(self,product,quantity):
        self.requestQueue.append([product,quantity,'add'])
        return

    def removeProductRequest(self,product,quantity):
        self.requestQueue.append([product,quantity,'remove'])
        return    

    # processRequests : DB processes requests            
    def processRequests(self):
        while(1):
            while(self.requestQueue):
                tempRequest = self.requestQueue.popleft()
                if tempRequest[2] == 'add':
                    self.addProduct(self,tempRequest[0],tempRequest[0])
                else:
                    self.removeProduct(self,tempRequest[0],tempRequest[0])
            #Checks every second
            time.sleep(1)
    

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
        self.tradeCount = 0
        self.tradeListLock = Lock()

   # Gets the the proxy for specified address.
    def getRpc(self,neighbor):
        a = xmlrpc.client.ServerProxy('http://' + str(neighbor) + '/')
        return True, a

    def calculateLatency(self, timeStart, timeStop):
        self.latency += (timeStop - timeStart).total_seconds()
        self.requestCount += 1
        if self.requestCount % 1000 == 0:
            self.printOnConsole('Average latency of peer '+str(self.peerId)+': '+str(self.latency / self.requestCount)+' (sec/req)')
        
    # Starting Server    
    def startServer(self):
        # Start Server and register its functions.
        HostIp = '127.0.0.1'
        server = AsyncXMLRPCServer((HostIp,int(self.hostAddr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.transaction,'transaction')
        server.register_function(self.registerProducts,'registerProducts')
        server.serve_forever()
        #***********************Needs to be fixed***********************
        if self.db['role'] == 'Buyer':
            self.beginTrading()
        elif self.db['role'] == 'Seller':
            self.beginTrading()
        elif self.db['role'] == 'Trader':
            self.beginTrading()
        
    # beginTrading : For a seller, through this method they register there product at the trader. For buyer, they start lookup process for the products needed, in this lab every lookup process is directed at the trader and he sells those goods on behalf of the sellers.            
    def beginTrading(self):

        # Seller keeps registers products
        if self.db["Role"] == "Seller":
            connected,proxy = self.getRpc(self.trader["hostAddr"])
            for productName, productCount in self.db['Inv'].items():
                if productCount > 0:
                    sellerInfo = {'seller': {'peerId':self.peerId,'hostAddr':self.hostAddr,'productName':productName},'productName':productName,'productCount':productCount} 	
                if connected:
                    proxy.registerProducts(sellerInfo)
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
                        with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                            f.write(" ".join([str(datetime.datetime.now()),"Peer ",str(self.peerId), ": Requesting ",item,'\n']))
                        timeStart = datetime.datetime.now()
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
    
    def printOnConsole(self, msg):
        with addLock:
            print(msg)

    #Trader lookups the product that a buyer wants to buy and replies respective seller and buyer.
    def lookup(self,buyer_id,hostAddr,productName):
        tot_prod = 0
        for i in self.tradeList:
            if self.tradeList[i]['productName'] == productName:
                    tot_prod+=self.tradeList[i]['productCount']
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
                connected,proxy = self.getRpc(self.trader["hostAddr"]) 

            if len(self.db['shop']) == 0:	
                #print("No products with buyer",self.peerId)	
                productList = ["Fish","Salt","Boar"]	
                x = random.randint(0, 2)	
                self.db['shop'].append(productList[x])	

        elif self.db["Role"] == "Seller":	
            self.db['Inv'][productName] = self.db['Inv'][productName] - 1	
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(datetime.datetime.now()),str(self.peerId),"sold an item. Remaining items are:",str( self.db['Inv']),'\n']))	
            connected,proxy = self.getRpc(self.trader["hostAddr"]) 
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
    0:'{"Role": "DBServer","Inv":{},"shop":{}}',
    1:'{"Role": "Trader","Inv":{},"shop":{}}',
    2:'{"Role": "Trader","Inv":{},"shop":{}}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"]}',
    4:'{"Role": "Seller","Inv":{"Fish":5,"Boar":1,"Salt":2},"shop":{}}',
    5:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"]}',
    6:'{"Role": "Seller","Inv":{"Fish":30,"Boar":30,"Salt":3},"shop":{}}'
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
            #Start DB
            peer_local = peer(HostIp + ":" + str(port+0),0,None,json.loads(db_load[0]),totalPeers)
            db_thread = td.Thread(target=peer_local.startServer,args=()) # Start Server
            db_thread.start()    
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
