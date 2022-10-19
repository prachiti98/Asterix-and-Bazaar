
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc
import threading as t
import socket
import random
from threading import Lock
import os
import time, datetime
import sys
import datetime
import random

#Initializing variables
buyer,seller,fish,salt,boar  = 1,2,3,4,5
toGoodsStringName = {fish:'Fish', salt:'Salt', boar:'Boar'}
toRoleStringName = {buyer:'Buyer', seller:'Seller'}
# the port number of each RPC peer server is (PORT_START_NUM + peer_id)
portNumber = 49151
# the maximum quantity a seller can sell
maxUnits = 10
# waiting time for a buyer to receive responses from sellers
clientWaitTime = 4
#------------Change neighbor mapping here:----------------
#Initializing Graph for various nodes
nodeMapping = {
    3: [[False, True, True],
        [True, False,False],
        [True, False, False]],
    4: [[False, True, False, True],
        [True, False, True, True],
        [False, True, False, True],
        [True, True, True, False]],
    5: [[False, True, False, False, False],
        [True, False, True, False, False],
        [False, True, False, True, False],
        [False, False, True, False, True],
        [False, False, False, True, False]],
    6: [[False, True, False, False, False, False],
        [True, False, True, False, False, False],
        [False, True, False, True, False, False],
        [False, False, True, False, True, False],
        [False, False, False, True, False, True],
        [False, False, False, False, True, False]]
}
#---------------------------------------------------------
deployOnLocalhost = True

# number of peers to be initialized on a machine
# Note that the number of peers on each machine = totalPeers / '# of MACHINES'
MACHINES = [{
   
   'ip': 'localhost'
}, {
   
   'ip': 'localhost'
}]


addLock = Lock()
peerServerList = []
currentServer = 'localhost' 

class Peer(t.Thread):
    def __init__(self, peerId, role, neighbors):
        t.Thread.__init__(self)
        
        self.peerId = peerId
        self.role = role
        self.neighbors = neighbors
        
        self.latency = 0
        self.requestCount = 0
            
    # start the thread
    def run(self):
        server = t.Thread(target=self.initialiseRPCServer)
        server.start()
        if self.role == buyer:
            self.initialiseBuyer()
        else:
            self.initialiseSeller()
        
    def initialiseRPCServer(self):
        server = SimpleXMLRPCServer((currentServer, portNumber + self.peerId),
                    allow_none=True, logRequests=False)

        server.register_function(self.getCurrentTime)
        server.register_function(self.lookup)
        
        server.register_function(self.reply) 

        # Clients (BUYERS) don't have to listen to buy requests 
        if self.role != buyer:
            server.register_function(self.buy)
        
        server.serve_forever()

    
    def getCurrentTime(self):
            return datetime.datetime.now()

    def getPeerIdServer(self, peerId):
        
        addr = peerServerList[peerId] + str(portNumber + peerId)
        proxyServer = xmlrpc.client.ServerProxy(addr)
        try:
            proxyServer.getCurrentTime() #check if server is up and running and ready to accept requests
        except socket.error:
            self.printOnConsole('Failed to connect to host. Please check all hosts')
            return None
        except xmlrpc.client.Fault as err:
            self.printOnConsole('Proxy Server Error - code: '+str(err.faultCode)+', msg: '+str(err.faultString))
            pass
        except xmlrpc.client.ProtocolError as err:
            self.printOnConsole('Proxy Server Error - code: '+str(err.errcode)+', msg: '+str(err.errmsg))
            return None
          
        return proxyServer

    def initialiseBuyer(self):
        while True:
            #Sleep for Random time
            time.sleep(random.randint(3,5))

            # generate a buy request
            self.target = random.randint(fish, boar) #since 100 to 102 

            f = open("Peer"+str(self.peerId)+"/output.txt","a")
            f.write(str(datetime.datetime.now()) +" Peer " + str(self.peerId) +" plans to buy "+ str(toGoodsStringName[self.target]) + "\n")
            f.close()

            self.startBuyTime = datetime.datetime.now()
            self.responseTime = []

            # ask neighbors
            self.potentialSellers = []
            for neighborId in self.neighbors:
                thread = t.Thread(target=self.lookupUtil, args=(neighborId, self.target, hopCount, str(self.peerId)))
                thread.start()

            time.sleep(clientWaitTime) #waits a specific amount of time to receive replies
            
            #stopped buying good because no sellers 
            if self.potentialSellers == []:
                f = open("Peer"+str(self.peerId)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) +" Stopped buying "+toGoodsStringName[self.target]+ " because no sellers" + "\n")
                f.close()

            totalResponseTime = sum(self.responseTime)
            # check candidate sellers and trade, choose the first seller [one with least response time]
            for sellerId in self.potentialSellers:
                proxyServer = self.getPeerIdServer(sellerId)
                
                if proxyServer != None and proxyServer.buy(self.target,self.peerId):
                    #to do the calculation
                    self.printOnConsole(str(datetime.datetime.now())+" Peer "+str(self.peerId)+" buys "+str(toGoodsStringName[self.target])+" from peer "+str(sellerId)+"; avg. response time: "+str(totalResponseTime / len(self.potentialSellers))+" (sec/req)")
                    f = open("Peer"+str(self.peerId)+"/output.txt","a")
                    f.write(str(datetime.datetime.now()) + " Bought " + str(toGoodsStringName[self.target]) +" from peerID " + str(sellerId) + "\n")
                    f.write("The response time of the seller chosen : " +str(self.responseTime[0])+"\n")
                    f.write("The average response time for buying "+str(toGoodsStringName[self.target])+": "+str(totalResponseTime/len(self.potentialSellers))+" \n")
                    f.close()
                    break

    def printOnConsole(self, msg):
        with addLock:
            print(msg)

    # find the server and call the main lookup
    def lookupUtil(self, peerId, productName, hopCount, path):
        proxyServer = self.getPeerIdServer(peerId)
        if proxyServer != None:
            timeStart = datetime.datetime.now()
            proxyServer.lookup(productName, hopCount, path)
            timeEnd = datetime.datetime.now()
            self.calculateLatency(timeStart, timeEnd)

    def lookup(self, productName, hopCount, path):
        footprints = path.split('-')
        

        # have the product
        if self.role != buyer and productName == self.good:
            fromNeighborId = int(footprints[0])
            newPath = '' if len(footprints) == 1 else "-".join(footprints[1:])

            proxyServer = self.getPeerIdServer(fromNeighborId)
            if proxyServer != None:
                #print the reply in receiver's output
                f = open("Peer"+str(fromNeighborId)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) + " Peer " + str(self.peerId) + " has " + toGoodsStringName[productName] + "\n")
                f.close()
                thread = t.Thread(target=self.replyUtil, args=(fromNeighborId, self.peerId, productName, newPath))
                thread.start()
            return True

        # propagate the request
        for neighborId in self.neighbors:
            if str(neighborId) not in footprints: #to avoid a cycle
                newPath = str(self.peerId)+'-'+str(path) #till current
                #if neighbor exists but max hopcount reached, print in last peer's (current) directory and don't search for neighbors further.
                if(hopCount-1 == 0):
                    f = open("Peer"+str(self.peerId)+"/output.txt","a")
                    f.write(str(datetime.datetime.now()) + " Max Hopcount Reached, Can't lookup futher. Lookup stopped Path: "+ str(newPath) + "\n")
                    f.close()
                    return False
                #print the lookup propogated in the propogater's output 
                f = open("Peer"+str(self.peerId)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) + " Lookup for product "+toGoodsStringName[productName]+" propogated from peerID " +str(self.peerId) + " to peerID " + str(neighborId) +"\n")
                f.close()
                thread = t.Thread(target=self.lookupUtil,args=(neighborId, productName, hopCount-1, newPath))
                thread.start()

        return True

    def initialiseSeller(self):
        self.good = random.randint(fish, boar)
        self.goodQuantity = random.randint(1, maxUnits)
        self.goodLock = Lock()

        f = open("Peer"+str(self.peerId)+"/output.txt","a")
        f.write(str(datetime.datetime.now()) + " Selling " +str(toGoodsStringName[self.good])+': '+str(self.goodQuantity)+" Unit(s) \n")
        f.close()

    def calculateLatency(self, timeStart, timeStop):
        self.latency += (timeStop - timeStart).total_seconds()
        self.requestCount += 1
        if self.requestCount % 1000 == 0:
            self.printOnConsole('Average latency of peer '+str(self.peerId)+': '+str(self.latency / self.requestCount)+' (sec/req)')

    # for thread to execute
    def replyUtil(self, peerId, sellerId, productName, newPath):
        proxyServer = self.getPeerIdServer(peerId)
        if proxyServer != None:
            timeStart = datetime.datetime.now()
            proxyServer.reply(sellerId, productName, newPath)
            timeStop = datetime.datetime.now()
            self.calculateLatency(timeStart, timeStop)

    def reply(self, sellerId, productName, path):
        # 1. The reply request arrives to the buyer
        if len(path) == 0:
            # target product has been updated (timeout)
            if productName != self.target:
                return False

            response = datetime.datetime.now()
            self.responseTime.append((response - self.startBuyTime).total_seconds())
            self.potentialSellers.append(sellerId)
            
            #print the reply in receiver's (buyer) directory
            f = open("Peer"+str(self.peerId)+"/output.txt","a")
            f.write(str(datetime.datetime.now()) + " Received a reply from peerID " +str(sellerId) +"\n")
            f.close()

            return True
        
        # 2. Otherwise, a peer propagates the reply request
        footprints = path.split('-')
        neighborId = int(footprints[0])
        newPath = '' if len(footprints) == 1 else "-".join(footprints[1:])

        #propogate the reply (can be seller or buyer) print in sender's directory
        f = open("Peer"+str(self.peerId)+"/output.txt","a")
        f.write(str(datetime.datetime.now()) + " Propogate the reply to peerID " +str(neighborId) + " Path: " +str(path) +"\n")
        f.close()
        
        thread = t.Thread(target=self.replyUtil, args=(neighborId, sellerId, productName, newPath))
        thread.start()
        return True


    def buy(self, productName,buyerId):
        if productName != self.good:

            #print in buyer's directory
            f = open("Peer"+str(buyerId)+"/output.txt","a")
            f.write(str(datetime.datetime.now()) + " Too late! PeerID " +str(self.peerId)+ "'s "+toGoodsStringName[self.good]+" got sold out" +"\n")
            f.close()
            return False

        # sync
        with self.goodLock:
            if self.goodQuantity <= 0:
                #print in buyer's directory
                f = open("Peer"+str(buyerId)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) + " Too late! PeerID " +str(self.peerId)+ "'s "+toGoodsStringName[self.good]+" got sold out" +"\n")
                f.close()
                return False

            self.goodQuantity -= 1
            f = open("Peer"+str(self.peerId)+"/output.txt","a")
            f.write(str(datetime.datetime.now()) + " Remaining " +str(toGoodsStringName[self.good])+': '+str(self.goodQuantity)+" Unit(s) \n")
            f.close()
        
            if self.goodQuantity == 0:
                self.goodQuantity = random.randint(1, maxUnits)
                self.good = random.randint(fish, boar)
                f = open("Peer"+str(self.peerId)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) + " Selling new good " +str(toGoodsStringName[self.good])+': '+str(self.goodQuantity)+" Unit(s) \n")
                f.close()
                
        #print in buyer's directory
        f = open("Peer"+str(buyerId)+"/output.txt","a")
        f.write(str(datetime.datetime.now()) + " Hurry! PeerID " +str(self.peerId)+ " has "+toGoodsStringName[self.good]+" to sell"+"\n")
        f.close()
        return True

def getRandomRoles(totalPeers):
    roles = [1,2] #set first peer to buyer, second seller 

    #randomisze roles for other peers
    for _ in range(totalPeers-2):
        roles.append(random.randint(buyer,seller))
    return roles
    
#To check if the graph is a fully connected graph
def check_connected(totalPeers):
    def create_graph(totalPeers):
        graph = {}
        for index,i in enumerate(nodeMapping[totalPeers]):
            graph[index] = []
            for index2,j in enumerate(i):
                if j == True:
                    graph[index].append(index2)
        return graph
    graph =  create_graph(totalPeers) 
    def dfs(temp, v, visited):
        visited.add(v)
        temp.append(v)
        for i in graph[v]:
            if i not in visited:
                temp = dfs(temp, i, visited)
        return temp
    visited, c = set(), [] 
    for v in graph:
        if v not in visited:
            c.append(dfs([], v, visited))
    return False if len(c)>1 else True

if __name__ == "__main__":
    #pass the number of peers via command line argument
    totalPeers = int(sys.argv[1])
    role = getRandomRoles(totalPeers)
    if totalPeers<=2:
        print('Enter more than 2 peers!')
    elif(not check_connected(totalPeers)):
        print('Graph is not fully connected!')
    else:
        peerNeighborMap = nodeMapping[totalPeers]
        
        if(totalPeers == 3):
            hopCount = 1
        else:
            hopCount = 2
        
        print('Running on: '+currentServer)
        print('Number of nodes: '+str(totalPeers))
        print('Roles:'," ".join([toRoleStringName[i] for i in role]))
        print('Neighbor Graph:')
        for row in peerNeighborMap:
            print(row)

        print('Hopcount:' + str(hopCount))
        print("Marketplace is live! Check output.txt in PeerID directory to check the logging \n")
        print("Note: Peers are 0 indexed \n")

        peers = []
        
        #always print where the program is run
        for peerId in range(totalPeers):
            #check if directory exists for printing
            path = 'Peer'+str(peerId)
            
            #else create
            if(not os.path.isdir(path)):
                os.mkdir(path)
            else:
                if(os.path.isfile(path+"/output.txt")): #if output.txt exists delete it for a new run
                    os.remove(path+"/output.txt")

        # run on localhost
        if deployOnLocalhost:  
            for peerId in range(totalPeers):              
                neighbors = []
                for j in range(totalPeers):
                    if peerNeighborMap[peerId][j]:
                        neighbors.append(j)

                peerServerList.append('http://localhost:')
                peer = Peer(peerId, role[peerId], neighbors) #calls init
                peers.append(peer)
                peer.start()
        
        else:
            curr_machine_order = 0
            if totalPeers%2 == 0:
                num_of_peers_on_each_machine = int(totalPeers / len(MACHINES))
            else:
                num_of_peers_on_each_machine = int((totalPeers+1) / len(MACHINES))
            
            for i in range(len(MACHINES)):
                if currentServer == MACHINES[i]['ip']:
                    curr_machine_order = i
                
                peerId_start = num_of_peers_on_each_machine * i
                peerId_end   = min(peerId_start + num_of_peers_on_each_machine,totalPeers)
                for peerId in range(peerId_start, peerId_end):
                    peerServerList.append('http://' + MACHINES[i]['ip'] + ':')
                    neighbors = []
                    for j in range(totalPeers):
                        if peerNeighborMap[peerId][j]:
                            neighbors.append(j)

                    peer = Peer(peerId, role[peerId], neighbors)
                    peers.append(peer)
                    peer.start()
                

        # dont close the main thread until interrupt
        for peer in peers:
            try:
                peer.join()
            except KeyboardInterrupt:
                print("Keyboard Interrupted. Stopped the thread. Press Ctrl + C again to end all threads")
                