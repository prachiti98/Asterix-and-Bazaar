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
import datetime
from collections import deque
from collections import defaultdict
from multiprocessing import Process,Lock

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
    def __init__(self,hostAddr,programType):
        self.hostAddr = hostAddr
        self.peerId = 0 #0 will start at 0
        self.latency = 0
        self.requestCount = 0
        #Storing data here for now. Can shift to file.
        self.tradeList = {} 
        self.tradeCount = 0
        self.requestQueue = deque()
        self.oversellCount = 0
        self.programType = programType
        self.databaseLock = Lock() #todo can change to multiprocessing lock -> check difference
        
        # Starting Server    
    def startServer(self):
        # Start Server and register its functions.
        HostIp = '127.0.0.1'
        server = AsyncXMLRPCServer((HostIp,int(self.hostAddr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.addProductRequest,'addProductRequest')
        server.register_function(self.removeProductRequest,'removeProductRequest')
        server.register_function(self.getTradeList,'getTradeList')
        server.register_function(self.setTradeList,'setTradeList')
        server.serve_forever()

    def addProduct(self,sellerInfo):
        self.tradeList[str(sellerInfo['seller']['peerId'])+'_'+str(sellerInfo['seller']['productName'])] = sellerInfo 
        return

    def removeProduct(self,seller,productName):
        if self.programType == 'Synchronous':
            sellerList = []
            for peerId,sellerInfo in self.tradeList.items():
                if sellerInfo["productName"] == productName:
                    sellerList.append(sellerInfo["seller"])
            if len(sellerList) > 0:
                with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                        f.write(" ".join([str(datetime.datetime.now()),' Item present. Informing Trader.','\n']))
                seller = sellerList[0]  
                self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"]  = self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"] -1 
                return [1,seller]
        else:
            tot_prod = 0
            for i in self.tradeList:
                if self.tradeList[i]['productName'] == productName:
                        tot_prod+=self.tradeList[i]['productCount']
            if tot_prod <= 0:
                self.oversellCount +=1
                print("Oversell!")
                return #cant remove product, amount cant be negative
            sellerList = []
            for peerId,sellerInfo in self.tradeList.items():
                if sellerInfo["productName"] == productName:
                    sellerList.append(sellerInfo["seller"])
            if len(sellerList) > 0:
                with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                        f.write(" ".join([str(datetime.datetime.now()),' Item present. Informing Trader.','\n']))
                seller = sellerList[0]            
                self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"]  = self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"] -1 
                return 1
                #Can inform trader here if product sold or not. Reuturn 1 if sold else -1 
        with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(datetime.datetime.now()),' Item not present. Informing Trader.','\n']))
        return -1            

    def addProductRequest(self,sellerInfo):
        self.requestQueue.append([sellerInfo,'','add'])
        res = self.processRequests()
        return res

    def removeProductRequest(self,seller,productName):
        self.requestQueue.append([seller,productName,'remove'])
        res = self.processRequests()
        return res 

    def getTradeList(self):
        return self.tradeList

    def setTradeList(self,updatedTradeList):
        self.tradeList = updatedTradeList
        return

    # processRequests : DB processes requests            
    def processRequests(self):
        while(self.requestQueue):
            tempRequest = self.requestQueue.popleft()
            if tempRequest[2] == 'add':
                self.addProduct(tempRequest[0])
            else:
                result = self.removeProduct(tempRequest[0],tempRequest[1])
                if(result == False):
                    self.oversellCount +=1
                return result
    
# Defining peer
class peer:
    def __init__(self,hostAddr,peerId,db,totalPeers,traders,databaseHostAddress,programType):
        self.hostAddr = hostAddr
        self.peerId = peerId
        self.db = db 
        self.latency = 0
        self.requestCount = 0
        self.trader = traders 
        self.databaseHostAddress = databaseHostAddress
        self.tradeList = {}  #localcache
        self.tradeCount = 0
        self.programType = programType
        self.tradeListLock = Lock()
        self.tradeCountLock = Lock()
        self.failOccur = False

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
    def tradeCountIncrease(self):    
        with self.tradeCountLock:    
            self.tradeCount+=1
        
    # Starting Server    
    def startServer(self):
        # Start Server and register its functions.
        HostIp = '127.0.0.1'
        server = AsyncXMLRPCServer((HostIp,int(self.hostAddr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.transaction,'transaction')
        server.register_function(self.registerProducts,'registerProducts')
        server.register_function(self.updateTrader,'updateTrader')
        server.serve_forever()
        
            
    #beginTrading : For a seller, through this method they register there product at the trader. For buyer, they start lookup process for the products needed, in this lab every lookup process is directed at the trader and he contacts the database server and register
    def beginTrading(self):
        time.sleep(2) 
        # Seller registers products
        if self.db["Role"] == "Seller":
            if not self.failOccur:
                chosenTrader = self.trader[random.randint(0,1)]
            else:
                chosenTrader = self.trader[0]
            hostAddress = '127.0.0.1:'+str(10030+chosenTrader)
            connected,proxy = self.getRpc(hostAddress)
            for productName, productCount in self.db['Inv'].items():
                if productCount > 0:
                    sellerInfo = {'seller': {'peerId':self.peerId,'hostAddr':self.hostAddr,'productName':productName},'productName':productName,'productCount':productCount} 	
                    with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                        f.write(" ".join([str(datetime.datetime.now()),"Peer: ",str(self.peerId), "Registering: ",str(productName),"Quantity: ",str(productCount),'\n']))
                if connected:
                    proxy.registerProducts(sellerInfo)
                time.sleep(1)
        # If you are a buyer, wait 2 seconds for the seller to register the products before you begin purchasing.
        elif self.db["Role"] == "Buyer":
            # Sellers register the products during this time.
            time.sleep(2) 
            while len(self.db['shop'])!= 0: 
                time.sleep(random.randint(1,5))
                item = self.db['shop'][0]
                print(self,self.peerId,self.failOccur,self.trader)
                if not self.failOccur:
                    chosenTrader = self.trader[random.randint(0,1)]
                else:
                    print(self.trader[0])
                    chosenTrader = self.trader[0]
                hostAddress = '127.0.0.1:'+str(10030+chosenTrader)
                connected,proxy = self.getRpc(hostAddress)
                if connected:
                    #increment buyer
                    with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                        f.write(" ".join([str(datetime.datetime.now()),"Peer ",str(self.peerId), ": Requesting ",item,'\n']))
                    timeStart = datetime.datetime.now()
                    proxy.lookup(self.peerId,self.hostAddr,item)
                    timeEnd = datetime.datetime.now()
                    self.calculateLatency(timeStart, timeEnd)
        #Trader checks of other trader is alive
        elif self.db["Role"] == "Trader":
            #Let eveyone start up
            time.sleep(10) 
            while(1):
                self.requestCount+=1
                #Hardcoded since only 2 traders are considered and either and we have made a design decision that the first trader will fail.
                if self.peerId == 1:
                    isAlive = self.heartbeat('127.0.0.1:'+str(10032))
                    failureNode = 2
                else:
                    isAlive = self.heartbeat('127.0.0.1:'+str(10031))
                    failureNode = 1
                if not isAlive[0]:
                    for i in range(3,7):
                        connected,proxy = self.getRpc('127.0.0.1:'+str(10030+i))
                        if connected:
                            proxy.updateTrader(failureNode)
                time.sleep(1)

    #Not working for some reason
    def updateTrader(self,failureTrader):
        if not self.failOccur:
            self.trader.remove(failureTrader)
            self.failOccur = True
            print(self.peerId,'Changing!',self.failOccur)
        return

    # registerProducts: Trader registers the seller goods.
    def registerProducts(self,sellerInfo): # Trader End.
        connected,proxy = self.getRpc(self.databaseHostAddress)
        if self.programType == 'Synchronous':
            with open('Peer_'+str(0)+".txt", "a") as f:
                f.write(" ".join([str(datetime.datetime.now()),"Adding to warehouse", "Product: ",str(sellerInfo['productName']),"Quantity: ",str(sellerInfo['productCount']),'\n']))  
            proxy.addProductRequest(sellerInfo)
        else:
            self.tradeList = proxy.getTradeList() #updated your own tradeList
            with self.tradeListLock:
                self.tradeList[str(sellerInfo['seller']['peerId'])+'_'+str(sellerInfo['seller']['productName'])] = sellerInfo 	
            logSeller(self.tradeList) #not sure
            proxy.addProductRequest(sellerInfo) #update the data warehouse - only with new info
    
    def printOnConsole(self, msg):
        with addLock:
            print(msg)

    #Trader lookups the product that a buyer wants to buy and replies respective seller and buyer.
    def lookup(self,buyer_id,hostAddr,productName):
        self.tradeCountIncrease()
        tot_prod = 0
        #Connects to database
        connected,databaseProxy = self.getRpc(self.databaseHostAddress)
        if connected:
            if self.programType == 'Synchronous':
                #Adds requests in database queue 
                itemPresent, seller =  databaseProxy.removeProductRequest('',productName)
                #Warehouse informs trader if item is present.
                if itemPresent == 1:
                    with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                        f.write(" ".join([str(datetime.datetime.now()),"Warehouse to Trader ",str(self.peerId)," -> Product present!",'\n']))  
                    connected,proxy = self.getRpc(hostAddr)
                    #Inform buyer and buyer removes product from shopping list
                    if connected: 
                        proxy.transaction(productName,seller,buyer_id,self.peerId)
                else:
                #Warehouse informs trader if item is not present.
                    with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                        f.write(" ".join([str(datetime.datetime.now()),"Recieved message from Warehouse that product is not present!",'\n']))
                #Trader informs buyer if item is not present.
                    with open('Peer_'+str(buyer_id)+".txt", "a") as f:
                        f.write(" ".join([str(datetime.datetime.now()),"Recieved message from Trader ",str(self.peerId)," that product is not present!",'\n']))
            else:
                self.tradeList = databaseProxy.getTradeList() #updated own cache
                for i in self.tradeList:
                    if self.tradeList[i]['productName'] == productName:
                            tot_prod+=self.tradeList[i]['productCount'] 
                sellerList = []
                for peerId,sellerInfo in self.tradeList.items():
                    if sellerInfo["productName"] == productName:
                        sellerList.append(sellerInfo["seller"])
            
                if len(sellerList) > 0:
                    # Log the request
                    seller = sellerList[0]
                    transactionLog = {str(self.tradeCount) : {'productName' : productName, 'buyer_id' : buyer_id, 'sellerId':seller,'completed':False}}
                    logTransaction('transactions.csv',transactionLog)
                    
                    with self.tradeListLock:
                        self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"]  = self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"] -1     	   
                    connected,proxy = self.getRpc(hostAddr)
                    if connected: # Pass the message to buyer that transaction is succesful
                        proxy.transaction(productName,seller,buyer_id,self.tradeCount)
                    
                    connected,proxy = self.getRpc(seller["hostAddr"])
                    # Pass the message to seller that its product is sold
                    if connected:
                        proxy.transaction(productName,seller,buyer_id,self.tradeCount)

                    completeTransaction('transactions.csv',transactionLog,str(self.tradeCount))

                    #update the datawarehouse
                    databaseProxy.removeProductRequest(seller,productName)
                else:
                    with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                        f.write(" ".join([str(self.peerId),"Item is not present!","\n"]))


	# transaction : Seller just deducts the product count, Buyer prints the message.    	
    def transaction(self, productName, sellerId, buyer_id, traderId):
        if self.db["Role"] == "Buyer":	
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(datetime.datetime.now()),"Receieved message from Trader ",str(traderId),"that item is available. Peer ", str(self.peerId), " : Bought ",productName, " from peer: ",str(sellerId["peerId"]),'\n']))
            if productName in self.db['shop']:	
                self.db['shop'].remove(productName)	
                if not self.failOccur:
                    chosenTrader = self.trader[random.randint(0,1)]
                else:
                    chosenTrader = self.trader[0]
            if len(self.db['shop']) == 0:	
                #print("No products with buyer",self.peerId)	
                productList = ["Fish","Salt","Boar"]	
                x = random.randint(0, 2)	
                self.db['shop'].append(productList[x])	
        elif self.db["Role"] == "Seller":	
            self.db['Inv'][productName] = self.db['Inv'][productName] - 1	
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(datetime.datetime.now()),str(self.peerId),"sold an item. Remaining items are:",str( self.db['Inv']),'\n']))	            	
            if not self.failOccur:
                chosenTrader = self.trader[random.randint(0,1)]
            else:
                chosenTrader = self.trader[0]
            hostAddress = '127.0.0.1:'+str(10030+chosenTrader)	
            connected,proxy = self.getRpc(hostAddress) 
            if self.db['Inv'][productName] == 0:	
                # Refill the item with seller               	
                x = random.randint(1, 10)	
                with open('Peer_'+str(self.peerId)+".txt", "a") as f:	
                    f.write(" ".join([str(self.peerId),"restocking",str(productName),'by',str(x),'more','\n']))
                self.db['Inv'][productName] = x	
                sellerInfo = {'seller': {'peerId':self.peerId,'hostAddr':self.hostAddr,'productName':productName},'productName':productName,'productCount':x}	
                if not self.failOccur:
                    chosenTrader = self.trader[random.randint(0,1)]
                else:
                    chosenTrader = self.trader[0]
                hostAddress = '127.0.0.1:'+str(10030+chosenTrader)
                connected,proxy = self.getRpc(hostAddress) 	
                if connected: 	
                    proxy.registerProducts(sellerInfo)	

    def heartbeat(self, hostAddress):
        #print('Heartbeat',self.requestCount,hostAddress)
        failurePeer = 1
        if self.requestCount >= 5 and hostAddress == '127.0.0.1:'+str(10030+failurePeer):
            print('Node down!')
            return False,'Failed!'
        else:
            a = xmlrpc.client.ServerProxy('http://' + str(hostAddress) + '/')
            return True, a
        

#Initial data        
db_load = {
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"]}',
    2:'{"Role": "Seller","Inv":{"Fish":5},"shop":{}}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"]}',
    4:'{"Role": "Seller","Inv":{"Fish":3,"Boar":1},"shop":{}}',
    5:'{"Role": "Buyer","Inv":{},"shop":["Salt","Fish","Fish","Fish","Fish","Fish","Fish"]}',
    6:'{"Role": "Seller","Inv":{"Salt":3},"shop":{}}'
}       

if __name__ == "__main__":
    port = 10030
    HostIp = '127.0.0.1'
    programType = sys.argv[1]
    totalPeers = len(db_load)
    #The first 2 nodes are always the traders
    traders = [1,2] 
    if programType == 'Synchronous':
        print("Synchronous marketplace is live! Check Peer_X.txt for logging!\n")
    else:
        print("Cached marketplace is live! Check Peer_X.txt for logging!\n")
    #Clearing the transactions file
    clr = ['transactions.csv','Peer_0.txt']
    for f in clr:
        try:
            os.remove(f)
        except OSError:
            pass
    if totalPeers<4:
        print('Less than 4 peers passed!')
    else:
        buyerCnt = sum([1 if 'Buyer' in db_load[i] else 0 for i in db_load])
        sellerCnt = sum([1 if 'Seller' in db_load[i] else 0 for i in db_load])
        if buyerCnt<1 or sellerCnt<1:
            print('Enter atleast 1 buyer and seller!')
        else:
            databaseHostAddress = HostIp+":"+str(10030)
            databaseConnection = database(databaseHostAddress,programType)
            # Start the DB Server
            process1 = Process(target=databaseConnection.startServer,args=()) 
            process1.start() 
            for peerId in range(1,totalPeers+1):
                hostAddr = HostIp + ":" + str(10030+peerId)
                peerId = peerId
                db = json.loads(db_load[peerId])
                num_peers = totalPeers
                peer_local = peer(hostAddr,peerId,db,totalPeers,traders,databaseHostAddress,programType)
                #Start the process for the traders
                if peerId in traders:
                    peer_local.db['Role'] = 'Trader'
                    process1 = Process(target=peer_local.startServer,args=()) 
                    process1.start()    
                    process2 = td.Thread(target=peer_local.beginTrading,args=())
                    process2.start() 
                #Start the process for the buyers and sellers
                else:
                    process1 = Process(target=peer_local.startServer,args=()) 
                    process1.start()    
                    process2 = td.Thread(target=peer_local.beginTrading,args=())
                    process2.start() 
                #Clearing the logging files
                try:
                    os.remove('Peer'+'_'+str(peerId)+'.txt')
                except OSError:
                    pass
                time.sleep(2)
                