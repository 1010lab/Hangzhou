from py2neo import Node,Graph,Relationship

graph = Graph("http://localhost:7474/", auth=("neo4j", "lml2000326"),name = "neo4j")
class TreeNode:
    #先不处理root属性
    def __init__(self,treeName,treeId,nodeIdLists,nodeNameLists,childrenLists = None) -> None:
        self.treeName = treeName
        self.treeId = treeId 
        self.nodeIdLists = nodeIdLists
        self.nodeNameLists = nodeNameLists
        self.childrenLists = childrenLists

    def _create(self):
        node = Node("vituralTree",name = self.treeName,  
        Id=self.treeId, nodeIdLists=self.nodeIdLists,
        nodeNameLists = self.nodeNameLists
        )
        graph.create(node)

    def add_nodeIdLists(self,id):
        self.nodeIdLists.append(id)

    def add_nodeNameLists(self,name):
        self.nodeNameLists.append(name)
    
    def equals(self,id):
        return self.treeId == id

def create_tree(item,id,name):
    treeId = item['id']
    treeName = item['nodeName']
    tree = TreeNode(treeName,treeId,[],[])
    tree.add_nodeIdLists(id)
    tree.add_nodeNameLists(name)
    return tree,treeId




    

   
        

