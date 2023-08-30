from flask import jsonify,make_response
from neo4j_query.query import Query
from flask_restful import Resource,reqparse
import time

q = Query()

class Converter():
    def __init__(self) -> None:
        self.nodes = []
        self.lines = []
        #维护一个列表来查看哪些node已经添加到self.nodes里了
        self.nodes_id = []
        
    #添加node信息
    def add_node(self,node):
        node_dict ={}
        properties = node.__dict__['_properties']
        node_dict["id"] = properties['nodeId']
        #如果该id存在，则不添加
        if self.find(properties['nodeId']):
            return
        node_dict["text"]  = properties["nodeName"]
        node_dict["info"]  = {"type":properties["type"],
                            "snType":properties["snType"],
                            "defaultColor":properties['defaultColor'] if properties.get('defaultColor') else "RGBA(255, 255, 255, 1)",
                            "remark":properties['remark']}
        self.nodes.append(node_dict)
        self.nodes_id.append(node['nodeId'])

    #添加line信息
    def add_relation(self,start_node,relation,end_node):
        line_dict = {}
        properties = relation.__dict__['_properties']
        line_dict["id"] = properties["relationId"]
        line_dict["from"] = start_node.__dict__['_properties']["nodeId"]
        line_dict["to"] = end_node.__dict__['_properties']["nodeId"]
        line_dict["text"] = properties["relationName"] if properties.get("relationName") else None
        line_dict["info"]  = {"relationType":properties["relationType"] ,
                            "treeId":properties["treeId"] if properties.get("treeId") else None,
                            "labelList":"未导入部分"}
        self.lines.append(line_dict)

    def find(self,id):
        return True if id in self.nodes_id else False
    
    def clear(self):
        self.nodes = []
        self.lines = []

class GraphQuery(Resource):
    def __init__(self) -> None:
        self.convert = Converter()
        self.items = []
        self.res = {}
   

    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
        for page,records in data.items():
            for record in records:
                start_node = record['start']
                relation = record['r']
                end_node = record['end']
                self.convert.add_node(start_node)
                
                if relation is not None:
                    self.convert.add_relation(start_node,relation,end_node)
                    self.convert.add_node(end_node)
            self.res[page+1]= {"nodes":self.convert.nodes, "lines":self.convert.lines,"totalPage":len(data.keys())}
            self.convert.clear()

    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        #添加参数label用于指定节点便签，当没有指定是默认为None
        parse.add_argument('label',choices=['body','instance'])
        parse.add_argument('siteID',required=True)
        parse.add_argument('pageSize',type= int,default= 10,help= "请输入int类型数据")
        parse.add_argument('pageNum',type= int,default= 1,help= "请输入int类型数据")
        args = parse.parse_args()
        res = q.graph_query(args.label,args.siteID,args.pageSize)
        self._convert_data(res)
        if len(res.keys()) == 0:
            return jsonify({"code":200,"message":"","data":{}})
        if len(res.keys()) < args.pageNum or args.pageNum ==0:
            response = make_response(r'''{"message":"请求页数出错"}''',400)
            return response
        answer = {"code":200,"message":"","data":self.res[args.pageNum]}
        return jsonify(answer)

class TreeQuery(Resource):
    def __init__(self) -> None:
        self.convert = Converter()
        self.items = []
        self.res = {}
   

    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
        for page,records in data.items():
            for record in records:
                start_node = record['start']
                relation = record['r']
                end_node = record['end']
                self.convert.add_node(start_node)
                if relation is not None:
                    self.convert.add_relation(start_node,relation,end_node)
                    self.convert.add_node(end_node)
            self.res[page+1]= {"nodes":self.convert.nodes, "lines":self.convert.lines,"totalPage":len(data.keys())}
            self.convert.clear()


    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('label',choices=['body','instance'])
        parse.add_argument('treeId',required=True)
        parse.add_argument('siteID',required=True)
        parse.add_argument('pageSize',type= int,help= "请输入int类型数据")
        parse.add_argument('pageNum',type= int,default= 1,help= "请输入int类型数据")
        args = parse.parse_args()
        res = q.tree_query(args.label,args.treeId,args.siteID,args.pageSize)
        self._convert_data(res)
        if len(res.keys()) == 0:
            return jsonify({"code":200,"message":"","data":{}})
        if len(res.keys()) < args.pageNum or args.pageNum ==0:
            response = make_response('''{"message":"请求页数出错"}''',400)
            return response
        answer = {"code":200,"message":"","data":self.res[args.pageNum]}
        return jsonify(answer)

class SetDefaultColor(Resource):
    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('nodeId',required=True,type = str, action = 'append')
        parse.add_argument('color',required=True)
        parse.add_argument('remark',type=str)
        args = parse.parse_args()
        res = q.set_default_color(args.nodeId,args.color,args.remark)
        answer = {"code":200,"message":"","data":args.color}
        return jsonify(answer)

class GetInstance(Resource):
    def __init__(self) -> None:
        self.convert = Converter()
   

    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
            for record in data:
                start_node = record['ins']
                self.convert.add_node(start_node)


    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('nodeId',required=True,type=str)
        args = parse.parse_args()
        res = q.get_instance(args.nodeId)
        self._convert_data(res)
        answer = {"code":200,"message":"","data":self.convert.nodes}
        return jsonify(answer)

class CountQuery(Resource):
    def __init__(self) -> None:
        pass

    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('label',choices=['body','instance'])
        parse.add_argument('siteID',required=True)
        args = parse.parse_args()
        res = q.count_query(args.label,args.siteID)
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