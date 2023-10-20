
class Converter():
    '''
        处理neo4j返回的转换类
            :param self.nodes: 存放需求格式的节点信息
            :param self.lines: 存放需求格式的边信息
            :param self.node_id: 存放已经添加的节点ID信息
            :param self.count_root: 用于统计在一页中出现的root节点的个数，在图查询中为虚拟root节点，在树查询中为所在虚拟树的根节点
    '''
    def __init__(self) -> None:
        self.nodes = []
        self.lines = []
        # 维护一个列表来查看哪些node已经添加到self.nodes里了
        self.nodes_id = []
        self.count_root = 0

    # 添加node信息
    def add_node(self, node=None):
        node_dict = {}
        label = str(node).split("rozenset({'")[1].split("'}")[0]
        properties = node.__dict__['_properties']
        node_dict["id"] = properties['nodeId']
        if self.find(properties['nodeId']):
            return
        node_dict["text"] = properties["nodeName"] if properties.get("nodeName") else None
        node_dict["info"] = {"type": properties["type"] if properties.get("type") else None,
                             "snType": properties["snType"] if properties.get("snType") else None,
                             "defaultColor": properties['defaultColor'] if properties.
                                get('defaultColor') else "RGBA(255, 255, 255, 1)",
                             "remark": properties['remark'] if properties.
                                get("remark") else None,
                             "fileType": properties['fileType'] if properties.
                                get("fileType") != "null" and properties.get("fileType") else None,
                             "label":label}
        self.nodes.append(node_dict)
        self.nodes_id.append(node['nodeId'])

    # 添加line信息
    def add_relation(self, start_node, relation, end_node):
        line_dict = {}
        properties = relation.__dict__['_properties']
        line_dict["id"] = properties["relationId"] if properties.get("relationId") else None
        line_dict["from"] = start_node.__dict__['_properties']["nodeId"]
        line_dict["to"] = end_node.__dict__['_properties']["nodeId"]
        line_dict["groupId"] = properties["groupId"] if properties.get("groupId") else None
        line_dict["text"] = properties["relationName"] if properties.get("relationName") else None
        line_dict["info"] = {"relationType": properties["relationType"] if properties.get("relationType") else None,
                             "treeId": properties["treeId"] if properties.get("treeId") else None,
                             "labelList": "未导入部分"}
        line_dict["lineType"] = properties["lineType"] if properties.get("lineType") else None
        self.lines.append(line_dict)

    #重载方法,由于python方法不能重载
    def add_relation_min_graph(self, start_id:str, relation, end_id:str):
        line_dict = {}
        properties = relation.__dict__['_properties']
        line_dict["id"] = properties["relationId"] if properties.get("relationId") else None
        line_dict["from"] = start_id
        line_dict["to"] = end_id
        line_dict["groupId"] = properties["groupId"] if properties.get("groupId") else None
        line_dict["text"] = properties["relationName"] if properties.get("relationName") else None
        line_dict["info"] = {"relationType": properties["relationType"] if properties.get("relationType") else None,
                             "treeId": properties["treeId"] if properties.get("treeId") else None,
                             "labelList": "未导入部分"}
        line_dict["lineType"] = properties["lineType"] if properties.get("lineType") else None
        self.lines.append(line_dict)

    # 在self.nodeId中查找当前id是否存在
    def find(self, id):
        return True if id in self.nodes_id else False

    def find_line(self, id):
        return True if id and id in [line_id.get("id") for line_id in self.lines] else False

    # 去除重复的边
    def unique_line(self):
        unique_dict = {}
        for item in self.lines:
            key = (item["from"],item["to"],item["groupId"])
            unique_dict[key] = item

        self.lines = list(unique_dict.values())
    

    #清空类中所有数据
    def deep_clear(self):
        self.nodes = []
        self.lines = []
        self.nodes_id = []

    # 用于重置页面，清空nodes和lines以及count_root
    def clear(self):
        self.nodes = []
        self.lines = []
        self.count_root = 0