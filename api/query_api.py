from flask import jsonify
from neo4j_query.query import Query
from flask_restful import Resource,reqparse
import time

q = Query()

class GraphQuery(Resource):
    def __init__(self) -> None:
        self.nodes = []
        self.lines = []
        #维护一个列表来查看哪些node已经添加到self.nodes里了
        self.nodes_id = []
        
    #添加node信息
    def add_node(self,node):
        node_dict ={}
        node_dict["id"] = node['nodeId']
        #如果该id存在，则不添加
        if self.find(node['nodeId']):
            return
        node_dict["text"]  = node["nodeName"]
        node_dict["info"]  = {"type":node["type"],
                            "snType":node["snType"],
                            "defaultColor":"default"}
        self.nodes.append(node_dict)
        self.nodes_id.append(node['nodeId'])

    #添加line信息
    def add_relation(self,start_node,relation,end_node):
        line_dict = {}
        line_dict["id"] = relation["relationId"]
        line_dict["from"] = start_node["nodeId"]
        line_dict["to"] = end_node["nodeId"]
        line_dict["info"]  = {"relationType":relation["relationType"],
                            "treeId":relation["treeId"],
                            "labelList":"未导入部分"}
        self.lines.append(line_dict)

    def find(self,id):
        return True if id in self.nodes_id else False

    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
        for record in data:
            start_node = record['start'].__dict__['_properties']
            relation = record['r'].__dict__['_properties']
            end_node = record['end'].__dict__['_properties']
            self.add_node(start_node)
            
            if relation is not None:
                self.add_relation(start_node,relation,end_node)
                self.add_node(end_node)
        

    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        #添加参数label用于指定节点便签，当没有指定是默认为None
        parse.add_argument('label',choices=['body','instance'])
        args = parse.parse_args()
        print(time.ctime())
        res = q.graph_query(args.label)
        print(time.ctime())
        self._convert_data(res)
        print(time.ctime())
        answer = {"code":200,"message":"success","data":{"nodes":self.nodes,
                                                        "lines":self.lines}}
        return jsonify(answer)

class TreeQuery(Resource):
    def __init__(self) -> None:
        self.nodes = []
        self.lines = []
        #维护一个列表来查看哪些node已经添加到self.nodes里了
        self.nodes_id = []
        
    #添加node信息
    def add_node(self,node):
        node_dict ={}
        node_dict["id"] = node['nodeId']
        #如果该id存在，则不添加
        if self.find(node['nodeId']):
            return
        node_dict["text"]  = node["nodeName"]
        node_dict["info"]  = {"type":node["type"],
                            "snType":node["snType"],
                            "defaultColor":"default"}
        self.nodes.append(node_dict)
        self.nodes_id.append(node['nodeId'])

    #添加line信息
    def add_relation(self,start_node,relation,end_node):
        line_dict = {}
        line_dict["id"] = relation["relationId"]
        line_dict["from"] = start_node["nodeId"]
        line_dict["to"] = end_node["nodeId"]
        line_dict["info"]  = {"relationType":relation["relationType"],
                        "treeId":relation["treeId"],
                        "labelList":"未导入部分"}
        self.lines.append(line_dict)

    def find(self,id):
        return True if id in self.nodes_id else False

    def _convert_data(self,data):
        
        #查询结果进行处理，处理成前段需要的格式
        for record in data:
            start_node = record['start']
            relation = record['r']
            end_node = record['end']
            self.add_node(start_node)
            
            if relation is not None:
                self.add_relation(start_node,relation,end_node)
                self.add_node(end_node)
        

    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('label',choices=['body','instance'])
        parse.add_argument('treeId',required=True)
        args = parse.parse_args()
        res = q.tree_query(args.label,args.treeId)
        self._convert_data(res)
        answer = {"code":200,"message":"","data":{"nodes":self.nodes,
                                                        "lines":self.lines}}
        return jsonify(answer)

class CountQuery(Resource):
    def __init__(self) -> None:
        pass

    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('label',choices=['body','instance'])
        args = parse.parse_args()
        res = q.count_query(args.label)
        answer = {"code":200,"message":"","data":res}
        return jsonify(answer)

class OneHopQuery(Resource):
    def __init__(self) -> None:
        pass

    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('nodeId',type=str,required=True,help="查询节点不能为空")
        parse.add_argument('label',type=str,default=None)
        args = parse.parse_args()
        res = q.one_hop_query(args.nodeId,args.label)
        answer = {"code":200,"message":"","data":res}
        return jsonify(answer)

class ThreeHopQuery(Resource):
    def __init__(self) -> None:
        pass

    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('nodeId',type=str,required=True,help="查询节点不能为空")
        parse.add_argument('label',type=str,default=None)
        args = parse.parse_args()
        res = q.three_hop_query(args.nodeId,args.label)
        answer = {"code":200,"message":"","data":res}
        return jsonify(answer)      

class ByAttributeQuery(Resource):
    def __init__(self) -> None:
        pass

    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('attributeKey',type=str,required=True,help="属性不能为空")
        parse.add_argument('attributeValue',type=str,required=True,help="属性值不能为空")
        parse.add_argument('label',type=str,default=None)
        args = parse.parse_args()
        res = q.by_attribute_query(args.attributeKey,args.attributeValue,args.label)
        answer = {"code":200,"message":"","data":res}
        return jsonify(answer)      