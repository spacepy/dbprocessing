class Graph:
    def __init__(self,name=""):
        self.name = name
        self.list_neighbor = {}
        self.list_node = {}

    def __str__(self):
        nodes = self.nodes()
        for n in nodes:
            result = "\n" + "{0}, neighbors: {1}".format(n, self.neighbors(n))
        return result
    
    def add_node(self,node):
        self.list_node[node] = True

    def add_edge(self,node,nodebis):
        try :
            self.list_neighbor[node].append(nodebis)
        except :
            self.list_neighbor[node] = []
            self.list_neighbor[node].append(nodebis)
        #try :                              (can be added for undirected edges)
        #    self.list_neighbor[nodebis].append(node)
        #except :
        #    self.list_neighbor[nodebis] = []
        #    self.list_neighbor[nodebis].append(node)
            
    def neighbors(self,node):
        try :
            return self.list_neighbor[node]
        except :
            return []

    def contains(self,node):
        nodes = self.nodes()
        for n in nodes:
            if n == node:
                return True
            else:
                return False
                
        
    def nodes(self):
        return self.list_node.keys()

    def getNode(self, name):
        nodes = self.nodes()
        for n in nodes:
            if n == name:
                return n
            else:
                return None
    
    def delete_edge(self,node,nodebis):
        self.list_neighbor[node].remove(nodebis)
        self.list_neighbor[nodebis].remove(node)
        
    def delete_node(self,node):
        del self.list_node[node]
        try :
            for nodebis in self.list_neighbor[node] :
                self.list_neighbor[nodebis].remove(node)
            del self.list_neighbor[node]
        except :
            return "error"
