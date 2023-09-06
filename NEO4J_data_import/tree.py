from py2neo import Node,Graph,Relationship


class TreeNode:
    #先不处理root属性
    def __init__(self,treeName,treeId,childrenLists = None) -> None:
        self.treeName = treeName
        self.treeId = treeId 
        self.childrenLists = childrenLists

    def create_node(self,graph):
        node = Node('virtualTree',nodeName = self.treeName,  
        nodeId=self.treeId)
        graph.create(node)
        return node

    def create_relation(self,body_node,r,tree_node,graph):
        relation  = Relationship(body_node, r, tree_node)
        graph.create(relation)
    
    def equals(self,id):
        return self.treeId == id

class LabelColletionNode:
    '''
        标签组节点
    '''
    def __init__(self,treeName,treeId,labelIds,type,classificationIds,organizationId,childrenLists ) -> None:
        self.treeName = treeName
        self.treeId = treeId 
        self.childrenLists = childrenLists
        self.labelIds =labelIds
        self.type = type
        self.classificationIds = classificationIds
        self.organizationId = organizationId

    def create_relation(self,body_node,r,tree_node,label_list,graph):
        relation  = Relationship(body_node, r, tree_node) 
        for node in label_list:
            key = node['key']
            value = node['value']
            relation[key] = value
        graph.create(relation)

    #创建LabelCollection标签
    def create_node(self,graph):
        # label_node_list = self.create_label_node(graph)
        node = Node('labelCollection',nodeName = self.treeName,  
        nodeId=self.treeId, labelIds = self.labelIds,type = self.type, 
        classificationIds = self.classificationIds,
        organizationId = self.organizationId)
        graph.create(node)
        return node

#通过json数据来创建对应的TreeNode对象
def create_tree_node(item): 
    treeId = item['id']
    treeName = item['nodeName'] if item.get('nodeName') else item['name']
    tree = TreeNode(treeName,treeId)
    return tree,treeId

#通过json数据来创建对应的LabelColletionNode对象
def create_label_col_node(item):
    treeId = item['id']
    treeName = item['nodeName'] if item.get('nodeName') else item['name']
    childrenLists = item['children']
    labelIds = item['labelIds']
    type = item['type']
    classificationIds = item['classificationIds']
    organizationId = item['organizationId']
    tree = LabelColletionNode(treeName,treeId,
                    labelIds,type,classificationIds,
                    organizationId,childrenLists)
    return tree,treeId


    

   
        

