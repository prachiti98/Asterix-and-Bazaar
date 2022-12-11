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
import os.path
import json
import datetime
from collections import deque
from collections import defaultdict
from multiprocessing import Process,Lock
import pandas as pd

addLock = Lock()
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
    def __init__(self,hostAddr,peerId,db,totalPeers,traders,databaseHostAddress,programType,noReach):
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
        self.noReach = noReach
        self.fileLock = Lock()

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

        
    # Starting Server    
    def startServer(self):
        # Start Server and register its functions.
        HostIp = '127.0.0.1'
        server = AsyncXMLRPCServer((HostIp,int(self.hostAddr.split(':')[1])),allow_none=True,logRequests=False)
        server.register_function(self.lookup,'lookup')
        server.register_function(self.transaction,'transaction')
        server.register_function(self.registerProducts,'registerProducts')
        server.register_function(self.updateTrader,'updateTrader')
        thread = td.Thread(target=self.beginTrading,args=())
        thread.start() 
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
                time.sleep(1)
                item = self.db['shop'][0]
                if not self.failOccur:
                    chosenTrader = self.trader[random.randint(0,1)]
                else:
                    chosenTrader = self.trader[0]
                hostAddress = '127.0.0.1:'+str(10030+chosenTrader)
                connected,proxy = self.getRpc(hostAddress)
                if connected:
                    #increment buyer
                    with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                        f.write(" ".join([str(datetime.datetime.now()),"Peer",str(self.peerId), ": Requesting",item,'From Trader',str(chosenTrader),'\n']))
                    timeStart = datetime.datetime.now()
                    proxy.lookup(self.peerId,self.hostAddr,item)
                    timeEnd = datetime.datetime.now()
                    self.calculateLatency(timeStart, timeEnd)
        #Trader checks of other trader is alive
        elif self.db["Role"] == "Trader":
            #Let eveyone start up
            while(1):
                self.requestCount+=1
                #Hardcoded since only 2 traders are considered and we have made a design decision that the first trader will fail.
                if self.peerId == 1:
                    isAlive = self.heartbeat('127.0.0.1:'+str(10032))
                    failureNode = 2
                else:
                    isAlive = self.heartbeat('127.0.0.1:'+str(10031))
                    failureNode = 1
                if not isAlive[0]:
                    #Check if any requests came in during failure
                    with self.tradeCountLock: 
                        df = pd.read_csv('transactions.csv')
                        df_g = df.groupby(['tradeCount','traderId','buyerId','productName'])
                        df_c = df_g.count()
                        failed_transaction = df_c[df_c['completed']==1]
                        if not failed_transaction.empty:
                            transactionId,traderId,buyerId,productName = failed_transaction.index[0][0],failed_transaction.index[0][1],failed_transaction.index[0][2],failed_transaction.index[0][3]
                            if self.noReach == 'F':
                                with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                                    f.write(" ".join([str(datetime.datetime.now()),"Trader ",str(self.peerId), " informed the buyer ",str(buyerId)," that transaction ",str(transactionId),'is successful','\n'])) 
                                connected,proxy = self.getRpc('127.0.0.1:'+str(10030+buyerId))
                                if connected: # Pass the message to buyer that transaction is succesful
                                    proxy.transaction(str(productName),'',int(buyerId),self.peerId)
                                transactionLog = {'tradeCount':str(transactionId),'traderId':traderId,'productName' : productName, 'buyerId' : buyerId,'completed':'FORWARD'}
                                df = pd.DataFrame(transactionLog,index=[0])
                                if os.path.exists('transactions.csv'):
                                    df.to_csv('transactions.csv',mode = 'a',header=False)
                                else:
                                    df.to_csv('transactions.csv',mode = 'a',header=True)
                                connected,databaseProxy = self.getRpc(self.databaseHostAddress)
                                databaseProxy.removeProductRequest('',productName)
                    #Update the traders list for other peers
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
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(datetime.datetime.now()),"Informed that trader has failed and only remaining trader is",str(self.trader[0]),'\n']))
        return

    # registerProducts: Trader registers the seller goods.
    def registerProducts(self,sellerInfo): # Trader End.
        connected,proxy = self.getRpc(self.databaseHostAddress)
        if self.programType == 'Synchronous':
            with open('Peer_'+str(0)+".txt", "a") as f:
                f.write(" ".join([str(datetime.datetime.now()),"Adding to warehouse", "Product: ",str(sellerInfo['productName']),"Quantity:",str(sellerInfo['productCount']),'\n']))  
            proxy.addProductRequest(sellerInfo)
        else:
            self.tradeList = proxy.getTradeList() #updated your own tradeList
            with self.tradeListLock:
                self.tradeList[str(sellerInfo['seller']['peerId'])+'_'+str(sellerInfo['seller']['productName'])] = sellerInfo 	
            proxy.addProductRequest(sellerInfo) #update the data warehouse - only with new info
    
    def printOnConsole(self, msg):
        with addLock:
            print(msg)

    #Trader lookups the product that a buyer wants to buy and replies respective seller and buyer.
    def lookup(self,buyerId,hostAddr,productName):
        time.sleep(2)
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
                        f.write(" ".join([str(datetime.datetime.now()),"Recieved message from Warehouse that Product is present!",'\n']))  
                    connected,proxy = self.getRpc(hostAddr)
                    #Inform buyer and buyer removes product from shopping list
                    if connected: 
                        proxy.transaction(productName,seller,buyerId,self.peerId)
                else:
                #Warehouse informs trader if item is not present.
                    with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                        f.write(" ".join([str(datetime.datetime.now()),"Recieved message from Warehouse that product is not present!",'\n']))
                #Trader informs buyer if item is not present.
                    with open('Peer_'+str(buyerId)+".txt", "a") as f:
                        f.write(" ".join([str(datetime.datetime.now()),"Recieved message from Trader",str(self.peerId),"that product is not present!",'\n']))
            else:
                with self.tradeCountLock: 
                    self.tradeCount+=1
                    self.tradeList = databaseProxy.getTradeList() #updated own cache
                    for i in self.tradeList:
                        if self.tradeList[i]['productName'] == productName:
                                tot_prod+=self.tradeList[i]['productCount'] 
                    sellerList = []
                    for peerId,sellerInfo in self.tradeList.items():
                        if sellerInfo["productName"] == productName:
                            sellerList.append(sellerInfo["seller"])
                    #Check if sellers re present
                    if len(sellerList) > 0:
                        with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                            f.write(" ".join([str(datetime.datetime.now()),"Trader ",str(self.peerId), "has the product Product:",str(sellerInfo['productName']),'\n']))  
                        # Log the request
                        seller = sellerList[0]
                        transactionLog = {'tradeCount':str(self.tradeCount),'traderId':self.peerId,'productName' : productName, 'buyerId' : buyerId,'completed':False}
                        df = pd.DataFrame(transactionLog,index=[0])
                        if os.path.exists('transactions.csv'):
                            df.to_csv('transactions.csv',mode = 'a',header=False)
                        else:
                            df.to_csv('transactions.csv',mode = 'a',header=True)
                        time.sleep(5)
                        #Trader recieves request but does not process it yet. If requestCount>=20 then it is assumed to have failed. 'T' has to be passed as arg.
                        if (self.failOccur and self.peerId == 1) or (self.requestCount>=20 and self.peerId == 1) and self.noReach == 'T':
                            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                                f.write(" ".join([str(datetime.datetime.now()),"Trader ",str(self.peerId), "failed and did not deduct goods. Transaction",str(self.tradeCount),"also failed and could not inform the buyer",str(buyerId),"that it was successful",'\n'])) 
                        else:
                            with self.tradeListLock:
                                self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"]  = self.tradeList[str(seller['peerId'])+'_'+str(seller['productName'])]["productCount"] -1    
                            #Trader recieves request but has processed it. If requestCount>=20 then it is assumed to have failed. 'F' has to be passed as arg.
                            if (self.failOccur and self.peerId == 1) or (self.requestCount>=20 and self.peerId == 1) and self.noReach == 'F':
                                with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                                    f.write(" ".join([str(datetime.datetime.now()),"Trader ",str(self.peerId), "failed but deducted goods from the cache. The transaction",str(self.tradeCount)," failed and could not inform the buyer",str(buyerId),"that it was successful",'\n'])) 
                            else:
                                # Pass the message to buyer that transaction is succesful
                                connected,proxy = self.getRpc(hostAddr)
                                if connected: 
                                    proxy.transaction(productName,seller,buyerId,self.peerId)
                                connected,proxy = self.getRpc(seller["hostAddr"])
                                # Pass the message to seller that its product is sold
                                if connected:
                                    proxy.transaction(productName,seller,buyerId,self.peerId)
                                    transactionLog = {'tradeCount':str(self.tradeCount),'traderId':self.peerId,'productName' : productName, 'buyerId' : buyerId,'completed':'True'}
                                df = pd.DataFrame(transactionLog,index=[0])
                                if os.path.exists('transactions.csv'):
                                    df.to_csv('transactions.csv',mode = 'a',header=False)
                                else:
                                    df.to_csv('transactions.csv',mode = 'a',header=True)
                            #update the datawarehouse
                            databaseProxy.removeProductRequest(seller,productName)
                        time.sleep(2)
                    else:
                        with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                            f.write(" ".join([str(self.peerId),"Item is not present!","\n"]))


	# transaction : Seller just deducts the product count, Buyer prints the message.    	
    def transaction(self, productName, sellerId, buyerId, traderId):
        if self.db["Role"] == "Buyer":	
            with open('Peer_'+str(self.peerId)+".txt", "a") as f:
                f.write(" ".join([str(datetime.datetime.now()),"Receieved message from Trader ",str(traderId),"that item is available. Peer ", str(self.peerId), " : Bought ",productName, " from peer",'\n']))
            if productName in self.db['shop']:	
                self.db['shop'].remove(productName)	
                if not self.failOccur:
                    chosenTrader = self.trader[random.randint(0,1)]
                else:
                    chosenTrader = self.trader[0]
            if len(self.db['shop']) == 0:	
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

    #Heartbeat to check if the trader is alive
    def heartbeat(self, hostAddress):
        failurePeer = 1
        if self.requestCount >= 20 and hostAddress == '127.0.0.1:'+str(10030+failurePeer):
            return False,'Failed!'
        else:
            a = xmlrpc.client.ServerProxy('http://' + str(hostAddress) + '/')
            return True, a
        

#Initial data        
db_load = {
    1:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"]}',
    2:'{"Role": "Seller","Inv":{"Fish":5},"shop":{}}',
    3:'{"Role": "Buyer","Inv":{},"shop":["Fish","Fish","Fish","Fish","Fish","Fish","Fish"]}',
    4:'{"Role": "Seller","Inv":{"Fish":30,"Boar":100},"shop":{}}',
    5:'{"Role": "Buyer","Inv":{},"shop":["Salt","Fish","Fish","Fish","Fish","Fish","Fish"]}',
    6:'{"Role": "Seller","Inv":{"Salt":30},"shop":{}}'
}       

if __name__ == "__main__":
    port = 10030
    HostIp = '127.0.0.1'
    programType = sys.argv[1]
    totalPeers = len(db_load)
    noReach = sys.argv[2]
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
                peer_local = peer(hostAddr,peerId,db,totalPeers,traders,databaseHostAddress,programType,noReach)
                #Start the process for the traders
                if peerId in traders:
                    peer_local.db['Role'] = 'Trader'
                    process1 = Process(target=peer_local.startServer,args=()) 
                    process1.start()    
                #Start the process for the buyers and sellers
                else:
                    process1 = Process(target=peer_local.startServer,args=()) 
                    process1.start()    
                #Clearing the logging files
                try:
                    os.remove('Peer'+'_'+str(peerId)+'.txt')
                except OSError:
                    pass
                time.sleep(2)
                