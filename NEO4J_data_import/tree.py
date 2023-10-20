from py2neo import Node,Graph,Relationship
import time

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

    def create_relation(self,body_node,r,tree_node,label_list,node_type,graph):
        relation  = Relationship(body_node, r, tree_node) 
        for node in label_list:
            key = node['key']
            value = node['value']
            relation[key] = value
        relation['lineType'] = "labc-"+node_type
        graph.create(relation)

    #先创建对应的label的Node节点，再创建标签组与标签之间的关系
    def create_node(self,graph):
        label_node_list = self.create_label_node(graph)
        node = Node('labelCollection',nodeName = self.treeName,  
        nodeId=self.treeId, labelIds = self.labelIds,type = self.type, 
        classificationIds = self.classificationIds,
        organizationId = self.organizationId)
        graph.create(node)
        for label_node in label_node_list:
            relation = Relationship(label_node,'belong_to',node)
            relation['lineType'] = "lab-labc"
            graph.create(relation)
        return node
    
    #创建标签node对象，并获得所有的对象列表
    def create_label_node(self,graph):
        label_list = [LabelNode(children.get("name"), children.get("id"), children.get("dataType"),
                    children.get("type"), children.get("classificationIds"),
                    children.get("organizationId"),children.get("labelIdPath"),
                    children.get("dataIndex")) for children in self.childrenLists]
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