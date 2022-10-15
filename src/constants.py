BUYER  = 1
SELLER = 2

FISH = 100
SALT = 101
BOAR = 102

toGoodsStringName = {
    FISH: 'Fish',
    SALT: 'Salt',
    BOAR: 'Boar'
}

############ CONGIGURABLE ############
# set True if you want to deploy locally
# setting True omits NUM_OF_PEER_ON_EACH_MACHINE and MACHINES
deployOnLocalhost = True

# number of peers to be initialized on a machine
# Note that the number of peers on each machine = totalPeers / '# of MACHINES'
MACHINES = [{
    
    'ip': '128.119.243.164'
}, {
    
    'ip': '128.119.243.175'
}]

# number of total peers
totalPeers = 6

MAX_BUYER_NUM  = int(totalPeers/2) + 1
MAX_SELLER_NUM = int(totalPeers/2) + 1

# optional
# default port to start a RPC server
# the port number of each RPC peer server is (PORT_START_NUM + peer_id)
portNumber = 10070
# the maximum quantity a seller can sell
maxUnits = 10
# waiting time for a buyer to receive responses from sellers
clientWaitTime = 4

hopCount = 2

######## SELF-DEFINED MAP & ROLE ########
# if you would like to initialize peers by using TEST_ROLE and TEST_MAP,
# please set DEBUG to True
DEBUG = True

#adjacency matrix for role and neighbors
nodeMapping = {
    2: [[BUYER, SELLER],[[False, True],[True, False]]],
    3: [[BUYER, SELLER, BUYER],[[False, True, True],[True, False,False],[True, False, False]]],
    4: [[BUYER, SELLER, SELLER, BUYER],[[False, True, False, True],[True, False, True, True],[False, True, False, True],[True, True, True, False]]],
    5: [[BUYER, SELLER, SELLER, BUYER, BUYER],[[False, True, False, False, False],[True, False, True, False, False],[False, True, False, True, False],[False, False, True, False, True],[False, False, False, True, False]]],
    6: [[BUYER, SELLER, SELLER, SELLER, BUYER, BUYER],[[False, True, False, False, False, False],[True, False, True, False, False, False],[False, True, False, True, False, False],[False, False, True, False, True, False],[False, False, False, True, False, True],[False, False, False, False, True, False]]]
}
# case 1
# TEST_ROLE = [BUYER, SELLER, BUYER]
# TEST_MAP  = [
#     [False, True, False],
#     [True, False, True],
#     [False, True, False]
# ]

# case 2
TEST_ROLE = [BUYER, SELLER, SELLER, BUYER]
TEST_MAP  = [
    [False, True, False, True],
    [True, False, True, True],
    [False, True, False, True],
    [True, True, True, False]
]

# case 3
# TEST_ROLE = [BUYER, BUYER, SELLER, BUYER]
# TEST_MAP  = [
#     [False, True, False, True],
#     [True, False, True, True],
#     [False, True, False, True],
#     [True, True, True, False]
# ]

#case 4 fully connected
# TEST_ROLE = [BUYER, SELLER, BUYER, SELLER]
# TEST_MAP  = [
#     [False, True, True, True],
#     [True, False, True, True],
#     [True, True, False, True],
#     [True, True, True, False]
# ]

# case 5
# TEST_ROLE = [BUYER, BUYER, SELLER, SELLER, BUYER, BUYER]
# TEST_MAP  = [
#     [False, True, False, True, True, False],
#     [True, False, True, False, True, True],
#     [False, True, False, True, False, False],
#     [True, False, True, False, False, False],
#     [True, True, False, False, False, True],
#     [False, True, False, False, True, False]
# ]

# case 6
# TEST_ROLE = [BUYER, SELLER, BUYER, BUYER, BUYER, BUYER]
# TEST_MAP  = [
#     [False, True, False, True, True, False],
#     [True, False, True, False, True, True],
#     [False, True, False, True, False, False],
#     [True, False, True, False, False, False],
#     [True, True, False, False, False, True],
#     [False, True, False, False, True, False]
# ]

# case 7 fully connected
# TEST_ROLE = [BUYER, BUYER, BUYER, BUYER, BUYER, BUYER]
# TEST_MAP  = [
#     [False, True, True, True, True, True],
#     [True, False, True, True, True, True],
#     [True, True, False, True, True, True],
#     [True, True, True, False, True, True],
#     [True, True, True, True, False, True],
#     [True, True, True, True, True, False]
# ]