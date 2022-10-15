
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import xmlrpc
import threading as t
import socket
import random
from threading import Lock
import os
import time, datetime
import sys
from constants import *
import datetime


addLock = Lock()
peerServerList= []
currentServer = 'localhost' if deployOnLocalhost else socket.gethostbyname(socket.gethostname())

class Peer(t.Thread):
    def __init__(self, peerId, role, neighbors):
        t.Thread.__init__(self)
        
        self.peerId = peerId
        self.role = role
        self.neighbors = neighbors
        

        self.printOnConsole("Neighbors of peer %d: %s",
            (self.peerId, ','.join(str(x) for x in self.neighbors)))

        self.latency = 0
        self.requestCount = 0
            
    # start the thread
    def run(self):
        server = t.Thread(target=self.initialiseRPCServer)
        server.start()
        if self.role == BUYER:
            self.initialiseBuyer()

        else:
            self.initialiseSeller()
        
    def initialiseRPCServer(self):
        server = SimpleXMLRPCServer((currentServer, portNumber + self.peerId),
                    allow_none=True, logRequests=False)

        server.register_function(self.hello)
        server.register_function(self.lookup)
        
        server.register_function(self.reply) 

        # Clients (BUYERS) don't have to listen to buy requests 
        if self.role != BUYER:
            server.register_function(self.buy)
        
        server.serve_forever()

    def getPeerIdServer(self, peerId):
        addr = peerServerList[peerId] % (portNumber + peerId)
        
        proxyServer = xmlrpc.client.ServerProxy(addr)
        
        try:
            proxyServer.hello()       # check if the server proxyServer exists
        except xmlrpc.client.Fault as err:
            self.printOnConsole('proxyServer Error - code: %d, msg: %s', (err.faultCode, err.faultString))
            pass
        except xmlrpc.client.ProtocolError as err:
            self.printOnConsole('proxyServer Error - code: %d, msg: %s', (err.errcode, err.errmsg))
            return None
        except socket.error:
            self.printOnConsole('Protocol Error - Failed to connect to peer %d', peerId)
            return None
            
        return proxyServer

    def initialiseBuyer(self):
        while True:
            time.sleep(3)
            # generate a buy request
            self.target = random.randint(FISH, BOAR) #since 100 to 102 

            #remove
            #self.printOnConsole("[INIT] Peer %d plans to buy %s", (self.peerId, toGoodsStringName[self.target]))

            f = open("Peer"+str(self.peerId)+"/output.txt","a")
            f.write(str(datetime.datetime.now()) +" Peer " + str(self.peerId) +" plans to buy "+ str(toGoodsStringName[self.target]) + "\n")
            f.close()

            self.startBuyTime = datetime.datetime.now()
            self.responseTime = 0

            # ask neighbors
            self.potentialSellers = []
            for neighborId in self.neighbors:
                thread = t.Thread(target=self.lookupUtil,
                    args=(neighborId, self.target, hopCount, '%d' % self.peerId))
                thread.start()

            time.sleep(clientWaitTime) #waits a specific amount of time to receive replies
            
            #stopped buying good because no sellers 
            if self.potentialSellers == []:
                f = open("Peer"+str(self.peerId)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) +" Stopped buying "+toGoodsStringName[self.target]+ " because no sellers" + "\n")
                f.close()

            # check candidate sellers and trade, choose the first seller
            for sellerId in self.potentialSellers:
                proxyServer = self.getPeerIdServer(sellerId)
                if proxyServer != None and proxyServer.buy(self.target,self.peerId):

                    #to do the calculation
                    self.printOnConsole("%s Peer %d buys %s from peer %d; avg. response time: %f (sec/req)",
                        (datetime.datetime.now(), self.peerId, toGoodsStringName[self.target], sellerId,
                        (self.responseTime / len(self.potentialSellers))))

                    f = open("Peer"+str(self.peerId)+"/output.txt","a")
                    f.write(str(datetime.datetime.now()) + " Bought " + str(toGoodsStringName[self.target]) +" from peerID " + str(sellerId) + "\n")
                    f.write("The average response time: %f \n"%(self.responseTime/len(self.potentialSellers)))
                    f.close()
                    break
                
    # find the server and call the main lookup
    def lookupUtil(self, peerId, product_name, hopCount, path):
        proxyServer = self.getPeerIdServer(peerId)
        if proxyServer != None:
            timeStart = datetime.datetime.now()
            proxyServer.lookup(product_name, hopCount, path)
            timeEnd = datetime.datetime.now()
            self._report_latency(timeStart, timeEnd)

    def lookup(self, product_name, hopCount, path):
        footprints = path.split('-')
        
        # have the product
        if self.role != BUYER and product_name == self.good:
            fromNeighborId = int(footprints[0])
            new_path = '' if len(footprints) == 1 else "-".join(footprints[1:])

            proxyServer = self.getPeerIdServer(fromNeighborId)
            if proxyServer != None:
                #self.printOnConsole("Peer %d has %s!! Reply to peer %d",
                    #(self.peerId, toGoodsStringName[product_name], fromNeighborId))

                #print the reply in receiver's output
                f = open("Peer"+str(fromNeighborId)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) + " Peer " + str(self.peerId) + " has " + toGoodsStringName[product_name] + "\n")
                f.close()

                thread = t.Thread(target=self.replyUtil,
                    args=(fromNeighborId, self.peerId, product_name, new_path))
                thread.start()
            return True

        # discard
        if hopCount == 0:
            #self.printOnConsole("[LOOKUP stop] Peer %d final path: %s", (self.peerId, path))

            #print the reply in last peer's directory
            f = open("Peer"+str(self.peerId)+"/output.txt","a")
            f.write(str(datetime.datetime.now()) + " Max Hop count reached, Lookup stopped Path: "+ str(path) + "\n")

            f.close()
            return False

        # propagate the request
        for neighborId in self.neighbors:
            if str(neighborId) not in footprints: #to avoid a cycle
                new_path = "%d-%s" % (self.peerId, path)
                

                #self.printOnConsole("[LOOKUP propagate] Peer %d: (next) %d <- %d (curr) - %s (path)",
                #    (self.peerId, neighborId, self.peerId, path))

                #print the lookup propogated in the propogater's output 
                f = open("Peer"+str(self.peerId)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) + " Lookup for product "+toGoodsStringName[product_name]+" propogated from peerID " +str(self.peerId) + " to peerID " + str(neighborId) +"\n")
                
                f.close()

                thread = t.Thread(target=self.lookupUtil,
                    args=(neighborId, product_name, hopCount-1, new_path))
                thread.start()

        return True

    def initialiseSeller(self):
        self.good = random.randint(FISH, BOAR)
        self.goodQuantity = random.randint(1, maxUnits)
        self.goodLock = Lock()

        f = open("Peer"+str(self.peerId)+"/output.txt","a")
        f.write(str(datetime.datetime.now()) + " Selling " +str(toGoodsStringName[self.good])+': '+str(self.goodQuantity)+"\n")
        f.close()
        #should be printed on console for everyone to see
        #self.printOnConsole("[INIT] Peer %d sells %d units of %s",
            #(self.peerId, self.goodQuantity, toGoodsStringName[self.good]))


    def printOnConsole(self, msg, arg):
        with addLock:
            print(msg % arg)


    def _report_latency(self, timeStart, timeStop):
        self.latency += (timeStop - timeStart).total_seconds()
        self.requestCount += 1
        if self.requestCount % 50 == 0:
            self.printOnConsole('**** [PERFORMANCE] Average latency of peer %d: %f (sec/req) ****',
                (self.peerId, (self.latency / self.requestCount)))

    # for thread to execute
    def replyUtil(self, peerId, sellerId, product_name, newPath):
        proxyServer = self.getPeerIdServer(peerId)
        if proxyServer != None:
            timeStart = datetime.datetime.now()
            proxyServer.reply(sellerId, product_name, newPath)
            timeStop = datetime.datetime.now()
            self._report_latency(timeStart, timeStop)
  

    def hello(self):
        return True


    def lookup(self, product_name, hopCount, path):
        footprints = path.split('-')
        
        # have the product
        if self.role != BUYER and product_name == self.good:
            fromNeighborId = int(footprints[0])
            newPath = '' if len(footprints) == 1 else "-".join(footprints[1:])

            proxyServer = self.getPeerIdServer(fromNeighborId)
            if proxyServer != None:
                #self.printOnConsole("Peer %d has %s!! Reply to peer %d",
                    #(self.peerId, toGoodsStringName[product_name], fromNeighborId))

                #print the reply in receiver's output
                f = open("Peer"+str(fromNeighborId)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) + " Peer " + str(self.peerId) + " has " + toGoodsStringName[product_name] + "\n")
                f.close()

                thread = t.Thread(target=self.replyUtil,
                    args=(fromNeighborId, self.peerId, product_name, newPath))
                thread.start()
            return True

        # discard
        if hopCount == 0:
            #self.printOnConsole("[LOOKUP stop] Peer %d final path: %s", (self.peerId, path))

            #print the reply in last peer's directory
            f = open("Peer"+str(self.peerId)+"/output.txt","a")
            f.write(str(datetime.datetime.now()) + " Max Hop count reached, Lookup stopped Path: "+ str(path) + "\n")

            f.close()
            return False

        # propagate the request
        for neighborId in self.neighbors:
            if str(neighborId) not in footprints: #to avoid a cycle
                new_path = "%d-%s" % (self.peerId, path)
                

                #self.printOnConsole("[LOOKUP propagate] Peer %d: (next) %d <- %d (curr) - %s (path)",
                #    (self.peerId, neighborId, self.peerId, path))

                #print the lookup propogated in the propogater's output 
                f = open("Peer"+str(self.peerId)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) + " Lookup for product "+toGoodsStringName[product_name]+" propogated from peerID " +str(self.peerId) + " to peerID " + str(neighborId) +"\n")
                
                f.close()

                thread = t.Thread(target=self.lookupUtil,
                    args=(neighborId, product_name, hopCount-1, new_path))
                thread.start()

        return True


    def reply(self, sellerId, product_name, path):
        # 1. The reply request arrives to the buyer
        if len(path) == 0:
            # target product has been updated (timeout)
            if product_name != self.target:
                return False

            t_response = datetime.datetime.now()
            self.responseTime += (t_response - self.startBuyTime).total_seconds()
            self.potentialSellers.append(sellerId)
            #self.printOnConsole("[RECEIVE] Peer %d receives a reply from peer %d", (self.peerId, sellerId))
            
            #print the reply in receiver's (buyer) directory
            f = open("Peer"+str(self.peerId)+"/output.txt","a")
            f.write(str(datetime.datetime.now()) + " Received a reply from peerID " +str(sellerId) +"\n")
            
            f.close()

            return True
        
        # 2. Otherwise, a peer propagates the reply request
        footprints = path.split('-')
        next_neighborId = int(footprints[0])
        new_path = '' if len(footprints) == 1 else "-".join(footprints[1:])

        #self.printOnConsole("[REPLY propagate] Peer %d: (curr) %d -> %d (next) --> %s (path)",
        #    (self.peerId, self.peerId, next_neighborId, new_path))

        #propogate the reply (can be seller or buyer) print in sender's directory
        f = open("Peer"+str(self.peerId)+"/output.txt","a")
        f.write(str(datetime.datetime.now()) + " Propogate the reply to peerID " +str(next_neighborId) + " Path: " +str(path) +"\n")
        
        f.close()
        
        thread = t.Thread(target=self.replyUtil, args=(next_neighborId, sellerId, product_name, new_path))
        thread.start()

        return True


    def buy(self, product_name,buyer_id):
        if product_name != self.good:
            #self.printOnConsole("[FAILURE] The %s of peer %d has been sold out.",
             #       (toGoodsStringName[self.good], self.peerId))

            #print in buyer's directory
            f = open("Peer"+str(buyer_id)+"/output.txt","a")
            f.write(str(datetime.datetime.now()) + " Too late! PeerID " +str(self.peerId)+ "'s "+toGoodsStringName[self.good]+" got sold out" +"\n")
            
            f.close()
            return False

        # sync
        with self.goodLock:
            if self.goodQuantity <= 0:
                #self.printOnConsole("[FAILURE] The %s of peer %d has been sold out.",
                #    (toGoodsStringName[self.good], self.peerId))

                #print in buyer's directory
                f = open("Peer"+str(buyer_id)+"/output.txt","a")
                f.write(str(datetime.datetime.now()) + " Too late! PeerID " +str(self.peerId)+ "'s "+toGoodsStringName[self.good]+" got sold out" +"\n")
                
                f.close()
                return False

            self.goodQuantity -= 1
        
            if self.goodQuantity == 0:
                self.goodQuantity = random.randint(1, maxUnits)
                self.good = random.randint(FISH, BOAR)
                
        #self.printOnConsole("[UPDATE] Peer %d has %d units of %s to sell",
         #   (self.peerId, self.goodQuantity, toGoodsStringName[self.good]))

        #print in buyer's directory
        f = open("Peer"+str(buyer_id)+"/output.txt","a")
        f.write(str(datetime.datetime.now()) + " Hurry! PeerID " +str(self.peerId)+ " has "+toGoodsStringName[self.good]+" to sell"+"\n")
        
        f.close()
        return True


# def generate_peerNeighborMap():
#     peerNeighborMap = []
#     for j in range(totalPeers):
#         row = [False for i in range(totalPeers)]
#         peerNeighborMap.append(row)

#     # generate neighbor map
#     for j in range(totalPeers):
#         # make sure that a peer always has a neighbor
#         assured = random.randint(0, j-1) if j == totalPeers-1 else random.randint(j+1, totalPeers-1)
#         peerNeighborMap[j][assured] = True
#         peerNeighborMap[assured][j] = True

#         for i in range(j+1, totalPeers-1):
#             if random.randint(0, 1) == 1:
#                 peerNeighborMap[j][i] = True
#                 peerNeighborMap[i][j] = True

#     return peerNeighborMap


# def generate_peer_roles():
#     buyer_num  = 0
#     seller_num = 0
#     roles = []

#     for i in range(totalPeers):
#         tmp = BUYER
#         while True:
#             tmp = random.randint(BUYER)
#             if tmp == BUYER and buyer_num <= MAX_BUYER_NUM:
#                 buyer_num += 1
#                 break
#             elif tmp == SELLER and seller_num <= MAX_SELLER_NUM:
#                 seller_num += 1
#                 break

        
#         roles.append(tmp)
    
#     return roles
    

if __name__ == "__main__":
    no_nodes = int(sys.argv[1])
    if no_nodes<2:
        print('Enter more than 1 node!')
    else:
        if DEBUG:
            # only support self-defined neighbor map
            role = nodeMapping[no_nodes][0]
            peerNeighborMap = nodeMapping[no_nodes][1]
            totalPeers = no_nodes
        # else:
        #     peerNeighborMap = generate_peerNeighborMap()
        #     role = generate_peer_roles()


        with addLock:
            print('Node is running on: %s\n' % currentServer)
            print('-----Peer Neighbor Adjacency List-----')
            for row in peerNeighborMap:
                print(row)
            print('\n')

        print("Check output.txt in PeerID directory to check the logging \n")


        peers = []
        # run at localhost
        if deployOnLocalhost:
            for peerId in range(totalPeers):
                #check if directory exists for printing
                path = 'Peer'+str(peerId)
                
                #else create
                
                if(not os.path.isdir(path)):
                    os.mkdir(path)
                else:
                    if(os.path.isfile(path+"/output.txt")): #if output.txt exists delete it for a new run
                        os.remove(path+"/output.txt")
                
                neighbors = []
                for j in range(totalPeers):
                    if peerNeighborMap[peerId][j]:
                        neighbors.append(j)

                peerServerList.append('http://localhost:%d')
                peer = Peer(peerId, role[peerId], neighbors) #calls init
                peers.append(peer)
                peer.start()
        
        # else:
        #     # find the order of current machine & create peerServerList
        #     # a machine with order 0 is the master machine
        #     curr_machine_order = 0
        #     num_of_peers_on_each_machine = int(totalPeers / len(MACHINES))
        #     for i in range(len(MACHINES)):
        #         if currentServer == MACHINES[i]['ip']:
        #             curr_machine_order = i
                
        #         peerId_start = num_of_peers_on_each_machine * i
        #         peerId_end   = peerId_start + num_of_peers_on_each_machine
        #         for peerId in range(peerId_start, peerId_end):
        #             peerServerList.append('http://' + MACHINES[i]['ip'] + ':%d')
            
        #     peerId_start = num_of_peers_on_each_machine * curr_machine_order
        #     peerId_end   = peerId_start + num_of_peers_on_each_machine
        #     for peerId in range(peerId_start, peerId_end):
        #         neighbors = []
        #         for j in range(totalPeers):
        #             if peerNeighborMap[peerId][j]:
        #                 neighbors.append(j)

        #         peer = Peer(peerId, role[peerId], neighbors)
        #         peers.append(peer)
        #         peer.start()

        # avoid closing main thread (why)
        for peer in peers:
            peer.join()