from py2neo import Node,Graph,Relationship


class TreeNode:
    #先不处理root属性
    def __init__(self,treeName,treeId,nodeIdLists,nodeNameLists,childrenLists = None) -> None:
        self.treeName = treeName
        self.treeId = treeId 
        self.nodeIdLists = nodeIdLists
        self.nodeNameLists = nodeNameLists
        self.childrenLists = childrenLists

    def create_node(self,graph):
        node = Node('virtualTree',nodeName = self.treeName,  
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

class LabelColletionNode:
    '''
        标签组节点
    '''
    def __init__(self,treeName,treeId,nodeIdLists,nodeNameLists,
                labelIds,type,classificationIds,organizationId,childrenLists ) -> None:
        self.treeName = treeName
        self.treeId = treeId 
        self.nodeIdLists = nodeIdLists
        self.nodeNameLists = nodeNameLists
        self.childrenLists = childrenLists
        self.labelIds =labelIds
        self.type = type
        self.classificationIds = classificationIds
        self.organizationId = organizationId


    def create_node(self,graph):
        label_node_list = self.create_label_node(graph)
        node = Node('labelCollection',nodeName = self.treeName,  
        nodeId=self.treeId, nodeIdLists=self.nodeIdLists, nodeNameLists = self.nodeNameLists,
        labelIds = self.labelIds,type = self.type, classificationIds = self.classificationIds,
        organizationId = self.organizationId)
        graph.create(node)
        for label_node in label_node_list:
            relation = Relationship(label_node,'belong_to',node)
            graph.create(relation)
        return node

    def add_nodeIdLists(self,id):
        self.nodeIdLists.append(id)

    def add_nodeNameLists(self,name):
        self.nodeNameLists.append(name)
    
    #解析LabelCollections下的children数据，找出每一个对应的标签，并创建LabelNode对象
    def _sparse(self):
        label_list = [LabelNode(children.get("name"), children.get("id"), children.get("dataType"),
                    children.get("type"), children.get("classificationIds"),
                    children.get("organizationId"),children.get("labelIdPath"),
                    children.get("dataIndex")) for children in self.childrenLists]
        self.childrenLists = [label.__dict__['nodeId'] for label in label_list]
        return label_list

    def create_label_node(self,graph):
        label_list = self._sparse()
        label_node_list = []
        for label_node in label_list:
            node = label_node.create_node(graph)
            label_node_list.append(node)
        return label_node_list


class LabelNode():
    def __init__(self,nodeName,nodeId,dataType,type,classificationIds,
                organizationId,labelIdPath,dataIndex) -> None:
        self.nodeName = nodeName
        self.nodeId = nodeId 
        self.dataType = dataType
        self.type = type
        self.classificationIds = classificationIds
        self.organizaztionId = organizationId
        self.labelIdPath = labelIdPath
        self.dataIndex = dataIndex

    def create_node(self,graph):
        node = Node('label',nodeName = self.nodeName,  
        nodeId=self.nodeId, dataType=self.dataType, type = self.type,
        classificationIds = self.classificationIds,organizationId = self.organizaztionId,
        labelIdPath = self.labelIdPath,dataIndex = self.dataIndex)
        graph.create(node)
        return node
        


def create_relation(body_node,r,tree_node,graph):
        relation  = Relationship(body_node, r, tree_node)
        graph.create(relation)


#通过json数据来创建对应的TreeNode对象
def create_tree(item,id,name): 
    treeId = item['id']
    treeName = item['nodeName'] if item.get('nodeName') else item['name']
    tree = TreeNode(treeName,treeId,[],[])
    tree.add_nodeIdLists(id)
    tree.add_nodeNameLists(name)
    return tree,treeId

#通过json数据来创建对应的LabelColletionNode对象
def create_label_col(item,id,name):
    treeId = item['id']
    treeName = item['nodeName'] if item.get('nodeName') else item['name']
    childrenLists = item['children']
    labelIds = item['labelIds']
    type = item['type']
    classificationIds = item['classificationIds']
    organizationId = item['organizationId']
    tree = LabelColletionNode(treeName,treeId,[],[],
                    labelIds,type,classificationIds,
                    organizationId,childrenLists)
    tree.add_nodeIdLists(id)
    tree.add_nodeNameLists(name)
    return tree,treeId


    

   
        

