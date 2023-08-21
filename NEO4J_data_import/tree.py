from py2neo import Node,Graph,Relationship

class TreeNode:
    '''
        虚拟树节点：
        treeName:虚拟树名称
        treeId:虚拟树id
        nodeIdList:该虚拟树上节点Id的列表(包括body,instance)
        nodeNameList:节点name的列表
        childrenLists：子树集合
    '''
    #先不处理root属性
    def __init__(self,treeName,treeId,nodeIdLists,nodeNameLists,childrenLists = None) -> None:
        self.treeName = treeName
        self.treeId = treeId 
        self.nodeIdLists = nodeIdLists
        self.nodeNameLists = nodeNameLists
        self.childrenLists = childrenLists

    def create_node(self,graph):
        node = Node("virtualTree",nodeName = self.treeName,  
        nodeId=self.treeId, nodeIdLists=self.nodeIdLists,
        nodeNameLists = self.nodeNameLists
        )
        graph.create(node)
        return node

    def add_nodeIdLists(self,id):
        self.nodeIdLists.append(id)

    def add_nodeNameLists(self,name):
        self.nodeNameLists.append(name)
    
    def equals(self,id):
        return self.treeId == id

#创建节点与虚拟树节点的关系
def create_relation(body_node,tree_node,graph):
        graph.create(tree_node)
        relation  = Relationship(body_node, "is_root", tree_node)
        graph.create(relation)

#用于创建虚拟树类对象
def create_tree(item,id,name):
    treeId = item['id']
    treeName = item['nodeName']
    tree = TreeNode(treeName,treeId,[],[])
    tree.add_nodeIdLists(id)
    tree.add_nodeNameLists(name)
    return tree,treeId