# Python3 implementation to find the
# shortest path in a directed
# graph from source vertex to
# the destination vertex

infi = 1000000000
   
# Class of the node
class Node:
   
    # Adjacency list that shows the
    # vertexNumber of child vertex
    # and the weight of the edge   
    def __init__(self, vertexNumber):       
        self.vertexNumber = vertexNumber
        self.children = []
   
    # Function to add the child for
    # the given node
    def Add_child(self, vNumber):   
        
        self.children.append(vNumber)
       
# Function to find the distance of
# the node from the given source
# vertex to the destination vertex
def dijkstraDist(g, s, path):
       
    # Stores distance of each
    # vertex from source vertex
    dist = [infi for i in range(len(g))]
   
    # bool array that shows
    # whether the vertex 'i'
    # is visited or not
    visited = [False for i in range(len(g))]
     
    for i in range(len(g)):       
        path[i] = -1
    dist[s] = 0
    path[s] = -1
    current = s
   
    # Set of vertices that has
    # a parent (one or more)
    # marked as visited
    sett = set()    
    while (True):
           
        # Mark current as visited
        visited[current] = True
        for i in range(len(g[current].children)): 
            
            v = g[current].children[i];           
            if (visited[v]):
                continue
   
            # Inserting into the
            # visited vertex
            sett.add(v)
            alt = dist[current] + 1
   
            # Condition to check the distance
            # is correct and update it
            # if it is minimum from the previous
            # computed distance
            if (alt < dist[v]):      
                dist[v] = alt
                path[v] = current;       
        if current in sett:           
            sett.remove(current);       
        if (len(sett) == 0):
            break
   
        # The new current
        minDist = infi
        index = 0
   
        # Loop to update the distance
        # of the vertices of the graph
        for a in sett:       
            if (dist[a] < minDist):          
                minDist = dist[a]
                index = a;          
        current = index;  
    return dist
   


    
     