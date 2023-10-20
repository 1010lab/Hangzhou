from flask import jsonify,make_response
from neo4j_query.query import Query
from flask_restful import Resource,reqparse
import math
from api.utils import *
from api.convert import  Converter
# from RDBS.db_utils import Mysql
import copy
q = Query()

#图查询
class GraphQuery(Resource):
    def __init__(self) -> None:
        self.convert = Converter()
        self.items = []
        self.res = {}

    def _convert_data(self, data):
        # 查询结果进行处理，处理成前段需要的格式
        for record in data:
            start_node = record['start']
            relation = record['r']
            end_node = record['end']
            self.convert.add_node(start_node)
            if relation is not None:
                self.convert.add_relation(start_node, relation, end_node)
                self.convert.add_node(end_node)
        self.res = {"nodes": self.convert.nodes, "lines": self.convert.lines}
        self.convert.clear()

    # 本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('label', choices=['body', 'instance'])
        parse.add_argument('siteID', required=True)
        args = parse.parse_args()
        res = q.graph_query(args.label, args.siteID)
        self._convert_data(res)
        answer = {"code": 200, "message": "", "data": self.res}
        return jsonify(answer)

class GraphQueryWithPage(Resource):
    def __init__(self) -> None:
        self.convert = Converter()

        self.res = {}

    def _convert_data(self, data, page, root_records):
        # 查询对应的页面以及查询结果
        items = []
        for record in data:
            start_node = record['start']
            relation = record['r']
            end_node = record['end']
            self.convert.add_node(start_node)
            if relation is not None:
                self.convert.add_relation(start_node, relation, end_node)
                self.convert.add_node(end_node)
        for record in root_records:
            start_node = record['n']
            relation = record['r']
            end_node = record['root']
            self.convert.add_node(start_node)
            if relation is not None:
                self.convert.add_relation(start_node, relation, end_node)
                self.convert.add_node(end_node)
        items = {"nodes": self.convert.nodes, "lines": self.convert.lines, "totalPage": page}
        return items

    # 本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        # 添加参数label用于指定节点便签，当没有指定是默认为None
        parse.add_argument('label', choices=['body', 'instance'])
        parse.add_argument('siteID', required=True)
        parse.add_argument('pageSize', type=int, default=10, help="请输入int类型数据")
        parse.add_argument('pageNum', type=int, default=1, help="请输入int类型数据")
        args = parse.parse_args()
        res = q.graph_query_with_page(args.label, args.siteID, args.pageSize, args.pageNum)
        root_records = q.create_root(args.siteID, args.pageSize, args.pageNum)
        page = math.ceil(q.count_query(args.label, args.siteID) / args.pageSize)
        items = self._convert_data(res, page, root_records)
        if page < args.pageNum or args.pageNum == 0:
            response = make_response(r'''{"message":"请求页数出错"}''', 400)
            return response
        items['rootId'] = str(args.pageNum) + "root"
        answer = {"code": 200, "message": "", "data": items}
        q.delete_root(str(args.pageNum) + "root")
        return jsonify(answer)

# 虚拟树查询
class TreeQuery(Resource):
    def __init__(self) -> None:
        self.convert = Converter()
        self.items = []
        self.nodes = []
        self.lines = []
        #虚拟树根节点列表
        self.root = []

    def _convert_data(self, data):
        # 查询结果进行处理，处理成前段需要的格式
            for record in data:
                start_node = record['start']
                relation = record['r']
                end_node = record['end']
                self.convert.add_node(start_node)
                if relation is not None:
                    self.convert.add_relation(start_node, relation, end_node)
                    self.convert.add_node(end_node)
            self.nodes.append(self.convert.nodes)
            self.lines.append(self.convert.lines)
            self.convert.clear()

    def _flatten(self):
        self.nodes = [element for sublist in self.nodes for element in sublist]
        self.lines = [element for sublist in self.lines for element in sublist]

    # 本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('label', choices=['body', 'instance'])
        parse.add_argument('treeIdList', required=True,type=str,action="append")
        parse.add_argument('siteID', required=True)
        args = parse.parse_args()
        for treeId in args.treeIdList:
            res = q.tree_query(args.label, treeId, args.siteID)
            # 查找虚拟树的根节点
            root = q.find_root(treeId)
            #添加虚拟树根节点
            rootId = root[0]['root'].__dict__['_properties']['nodeId'] if len(root) > 0 else []
            self.root.append(rootId)
            self._convert_data(res)
        # self._flatten()
        #创建虚拟root节点以及关系
        # vir_node,vir_lines = generate_root_data(self.root) 
        # self.nodes.append(vir_node)
        # for line in vir_lines:
        #     self.lines.append(line)
        # root = vir_node['id']
        # res = {"nodes": self.nodes, "lines": self.lines, "rootId": root}
        self.nodes = unique_node(self.nodes)
        self.lines = unique_line(self.lines)
        res = {"nodes": self.nodes, "lines": self.lines}
        answer = {"code": 200, "message": "", "data":res}
        return answer

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

#查询对应本体下的实体
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
        self.convert = Converter()
        self.nodes = []
        self.lines = []
   
    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
        for record in data:
            start_node = record['start']
            relation = record['r']
            end_node = record['end']
            self.convert.add_node(start_node)
            if relation is not None:
                self.convert.add_relation(start_node,relation,end_node)
                self.convert.add_node(end_node)
        self.convert.unique_line()
        self.nodes.append(self.convert.nodes)
        self.lines.append(self.convert.lines)
        self.convert.clear()

    def _flatten(self):
        self.nodes = [element for sublist in self.nodes for element in sublist]
        self.lines = [element for sublist in self.lines for element in sublist]

    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('nodeIdList',required=True,type=str,action="append")
        parse.add_argument('label',type=str)
        args = parse.parse_args()
        for nodeId in args.nodeIdList:
            res = q.one_hop_query(nodeId,args.label)
            self._convert_data(res)
        self._flatten()
        #创建虚拟root节点以及关系
        # vir_node,vir_lines = generate_root_data(args.nodeIdList)
        # self.nodes.append(vir_node)
        # for line in vir_lines:
        #     self.lines.append(line)
        # root = vir_node['id']
        # res = {"nodes": self.nodes, "lines": self.lines, "rootId": root}
        res = {"nodes": self.nodes, "lines": self.lines}
        answer = {"code": 200, "message": "", "data":res}
        return jsonify(answer)

class TypeQuery(Resource):
    def __init__(self) -> None:
        self.convert = Converter()
        self.items = []
        self.res = {}
   

    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
        for record in data:
            start_node = record['start']
            relation = record['r']
            end_node = record['end']
            self.convert.add_node(start_node)
            if relation is not None:
                self.convert.add_relation(start_node,relation,end_node)
                self.convert.add_node(end_node)
        self.convert.unique_line()
        self.res = {"nodes":self.convert.nodes, "lines":self.convert.lines}
        self.convert.clear()


    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('nodeId',required=True)
        parse.add_argument('type',required=True,choices=[0,1],type=int)
        args = parse.parse_args()
        res = q.type_query(args.nodeId,args.type)
        self._convert_data(res)
        answer = {"code":200,"message":"","data":self.res}
        return jsonify(answer)

class ShortestPathQury(Resource):
    def __init__(self) -> None:
        self.convert = Converter()
        self.items = []
        self.res = {}
   

    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
        for record in data:
            start_node = record['start']
            relation = record['r']
            end_node = record['end']
            self.convert.add_node(start_node)
            if relation is not None:
                self.convert.add_relation(start_node,relation,end_node)
                self.convert.add_node(end_node)
        self.convert.unique_line()
        self.res = {"nodes":self.convert.nodes, "lines":self.convert.lines}
        self.convert.clear()


    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('startNodeId',required=True)
        parse.add_argument('endNodeId',required=True)
        args = parse.parse_args()
        res = q.shortest_path_query(args.startNodeId,args.endNodeId)
        self._convert_data(res)
        answer = {"code":200,"message":"","data":self.res}
        return jsonify(answer)

#根据siteID删除要素图谱以及资料包图谱中的数据
class DeleteGraph(Resource):
    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('siteID',required=True)
        args = parse.parse_args()
        summary = q.delete_graph(args.siteID)
        #创建Mysql实例
        # mysql = Mysql()
        #删除mysql中的站点记录
        # mysql.delete(args.siteID)
        answer = {"code":200,"message":"","data":summary}
        return jsonify(answer)      

#获取节点信息
class GetNodeInfo(Resource):
    def __init__(self) -> None:
        self.convert = Converter()
   
    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
        for record in data:
            start_node = record['m']
            self.convert.add_node(start_node)
         
    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('nodeIdList',required=True,type=str,action="append")
        args = parse.parse_args()
        res = q.get_node_info(args.nodeIdList)
        self._convert_data(res)
        #创建虚拟root节点以及关系
        # vir_node,vir_lines = generate_root_data(args.nodeIdList)
        # self.convert.nodes.append(vir_node)
        # for line in vir_lines:
        #     self.convert.lines.append(line)
        # root = vir_node['id']
        # res = {"nodes": self.convert.nodes, "lines": self.convert.lines, "rootId": root}
        res = {"nodes": self.convert.nodes, "lines": self.convert.lines}
        answer = {"code": 200, "message": "", "data":res}
        return jsonify(answer)

#表结构：表内查询
class InStructureQuery(Resource):

    def __init__(self) -> None:
        self.convert = Converter()
        self.items = []
        self.res = {}
   

    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
        for record in data:
            start_node = record['start']
            relation = record['r']
            end_node = record['end']
            self.convert.add_node(start_node)
            if relation is not None:
                self.convert.add_relation(start_node,relation,end_node)
                self.convert.add_node(end_node)
        
    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('structureId',required=True)
        args = parse.parse_args()
        res = q.structure_query(args.structureId)
        self._convert_data(res)
        res = q.sc_ins_realtion_query(args.structureId)
        self._convert_data(res)
        res = q.sc_instance_query(args.structureId)
        self._convert_data(res)
        self.convert.unique_line()
        self.res = {"nodes":self.convert.nodes, "lines":self.convert.lines}
        answer = {"code":200,"message":"","data":self.res}
        return jsonify(answer)

#表结构：表外查询
class OutStructureQuery(Resource):
    
    def __init__(self) -> None:
        self.convert = Converter()
        self.tree_items = []
        self.nodes = []
        self.lines = []
        #虚拟树节点列表
        self.root = []
   

    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
        for record in data:
            start_node = record['start']
            
            relation = record['r']
            end_node = record['end']
            self.convert.add_node(start_node)
            if relation is not None:
                self.convert.add_relation(start_node,relation,end_node)
                self.convert.add_node(end_node)
        
    #查找虚拟树集合的交集
    def _find_vir_list(self,lists):
        if len(lists) == 0:
            return []
        intersection = set(lists[0] if lists[0] != ['null'] else lists[1])
        for i in range(1, len(lists)):
            if lists[i] != ['null']:
                intersection = intersection.intersection(set(lists[i]))
        return list(intersection)

    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('structureId',required=True)
        args = parse.parse_args()
        #获得该表结构的根节点nodeId
        str_root  = q.find_str_root(args.structureId)
        if not str_root: return jsonify({"code":200,"message":"","data":"没有该表结构"})
        #获得该表结构下的所有虚拟树集合
        vir_list = q.find_vir_list(args.structureId)
        vir_list  = self._find_vir_list(vir_list)
        #遍历虚拟树结合找到所有虚拟树的根节点nodeId
        for vir_nodeId in vir_list:
            #每次清楚一次表数据
            self.convert.clear()
            vir_root = q.find_vir_root(vir_nodeId)
            vir_name = q.find_vir_name(vir_nodeId)
            #先判断根节点是否在表内，执行表内的where语句。如果不在表内且根节点不同，则说明存在表外结构
            if vir_root == str_root or q.is_inner(vir_root,args.structureId): continue
            res = q.outer_query(vir_root,str_root)
            self._convert_data(res)
            res = q.outer_instance_query(vir_root,str_root)
            self._convert_data(res)
            res = q.outer_ins_relation_query(vir_root,str_root)
            # self._convert_data_ex(res)
            self._convert_data(res)
            self.convert.unique_line()
            self.root.append(vir_root)
            #若环形关系存在与Lines中则说明有反向关系
            circle_relation = q.circle_relation(args.structureId)
            if self.convert.find_line(circle_relation):
                #弹出对应的虚拟树根节点
                self.root.pop() 
                self.convert.clear()
            #若存在说明存在表外数据
            else:
                self.tree_items.append({"id":vir_nodeId,"virtualTreeName":vir_name}) 
            self.nodes.append(self.convert.nodes)
            self.lines.append(self.convert.lines)
        nodes,lines = unique_node(self.nodes),unique_line(self.lines)
        #生成虚拟的根节点以及关系
        vir_node,vir_lines = generate_root_data(self.root)
        nodes.append(vir_node)
        for line in vir_lines:
            lines.append(line)
        self.root = [vir_node['id']]
        res = {"nodes": nodes, "lines": lines,"virtualTree": self.tree_items,"rootId":self.root[0] if len(self.root)>0 else None}
        answer = {"code":200,"message":"","data":res}
        return jsonify(answer)

#最小子图查询
class MinimalGraph(Resource):
     
    def __init__(self) -> None:
        self.convert = Converter()
        self.tree_items = []
        self.nodes = []
        self.lines = []
        self.root = []

    def is_instance(self,node_list):
        for node_id in node_list:
            ##如果该节点不是instance则结束循环
            if q.find_instance(node_id) is None:
                return False
        return True

    def add(self,data):
        for record in data:
            start_node = record['start']
            relation = record['r']
            end_node = record['end']
            self.convert.add_node(start_node)
            if relation is not None:
                self.convert.add_relation(start_node,relation,end_node)
                self.convert.add_node(end_node)

        #返回bodyid
        return end_node.__dict__['_properties']["nodeId"]

    '''
        双队列存在的问题：存在边或顶点覆盖的问题，可能的解决方式：
        1.直接将其加入到节点中，再去重
        2.使用集合set
    '''
    def traverse(self,node_list,siteID,instance_access:bool):
        #用于存放nodeId
        nodes_map = {}
        lines_map = {}
        length = len(node_list)
        # #定义两个列表用于存放body以及instance
        # instance_list = [None]*length
        # body_list = [None]*length
        # # 若为实体节点则转向对应的body节点并使用max_level减一
        # if not instance_access:
        #     for i in range(length):
        #         body = q.get_body(node_list[i])
        #         if body:
        #             #找出实体本体关系以及对应本体id
        #             instance_list[i] = self.add_line(body)
        #             body_list[i] = instance_list[i]["line"][0].split(',')[-1]
        #         else:
        #             body_list[i] = node_list[i]
        #     node_list = body_list
        #     #当只有一个body节点时m,只需找出对应的body节点
        #     if len(list(set(body_list))) == 1:
        #         node = body[0]['end']
        #         ID = node.__dict__.get('_properties')['nodeId']
        #         if ID not in nodes_map: nodes_map[ID] = node
        for i in range(len(node_list)-1):
            #节点id
            start_id = node_list[i]
            #当instance_accesss=False时，找到其body节点
            max_level = 7
            if not instance_access:
                triple = q.get_body(node_list[i])
                if triple:
                    max_level -= 1
                    #将body、instance、以及他们之间的关系引入到Convert类中
                    start_id = self.add(triple)
            #找到两个点的可达路径中的最短边
            for j in range(i+1,len(node_list)):
                end_id = node_list[j]
                if not instance_access:
                    triple = q.get_body(node_list[j])
                    if triple:
                        max_level -= 1
                        #将body、instance、以及他们之间的关系引入到Convert类中
                        end_id = self.add(triple)
                #若开始节点和终止节点都在map中则证明该节点已经包含在图中
                if(start_id in nodes_map and end_id in nodes_map):continue
                # if body_id: res = q.accessibility(body_id,end_id,max_level,siteID)
                if instance_access:
                    res = q.instance_accessibility(start_id,end_id,max_level,siteID)
                else:
                    res = q.accessibility(start_id,end_id,max_level,siteID)
                #若无结果则说明无最小子图
                #该部分存在问题：若对应一个节点到列表中其他节点不可达，则应该返回为空（目前还没达到）
                #遍历节点查找出对应的节点nodeId
                #可以省略，直接查出relation
                if not res: continue
                for node in res[0]['nodes']:
                    ID = node.__dict__.get('_properties')['nodeId']
                    if ID not in nodes_map: nodes_map[ID] = node
                for relation in res[0]['relations']:
                    r_dict = relation.__dict__
                    start_node = r_dict.get('_start_node')
                    inner_start_id =  start_node.__dict__.get('_properties')['nodeId']
                    end_node = r_dict.get('_end_node')
                    inner_end_id =  end_node.__dict__.get('_properties')['nodeId']
                    line_key = inner_start_id+','+inner_end_id
                    if line_key in lines_map:continue
                    lines_map[line_key] = relation
        #添加instane的节点以及边到nodes_map中
        # for item in instance_list:
        #     #若不为空
        #     if item:
        #         node = item.get("node")
        #         ID = node.__dict__.get('_properties')['nodeId']
        #         if ID in nodes_map: nodes_map[ID] = node
        #         line = item.get("line")
                
        #         lines_map[line[0]] = line[1]
        return nodes_map,lines_map

    def _convert_data(self,nodes_map,lines_map):
        #查询结果进行处理，处理成前段需要的格式
        for _,node in nodes_map.items():
            self.convert.add_node(node)
        for tup,line in lines_map.items():
            #拿出startNode和endNode的id
            start_id,end_id = tup.split(',')
            self.convert.add_relation_min_graph(start_id,line,end_id) 

    def process(self,args):
        #在进行节点列表的最短路径遍历之前，应先判断节点列表是否都为instance
        all_instance = self.is_instance(args.nodeList) #bool
        #若全为intance则查找instance间的最短路径,并且找出其本体路径
        if all_instance: 
            ins_nodes_map,ins_lines_map = self.traverse(args.nodeList,args.siteID,True)
            self._convert_data(ins_nodes_map,ins_lines_map)
            instance_result = {"nodes":self.convert.nodes,"lines":self.convert.lines}
        #若不全为intance则直接返回空数据
        else:
            instance_result = {"nodes":[],"lines":[]}
        #清空结果,包括记录的存在id
        self.convert.deep_clear()
        #查找节点列表中的最小子图(不区分节点标签)
        nodes_map, lines_map = self.traverse(args.nodeList,args.siteID,False)
        self._convert_data(nodes_map,lines_map)
        self.convert.unique_line()
        all_shortest_res = {"nodes":self.convert.nodes,
                            "lines":self.convert.lines}
        return instance_result,all_shortest_res

    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('nodeList',required=True,type=str,action="append")
        parse.add_argument('siteID',required=True,type=str)
        args = parse.parse_args()
        instance_result,all_shortest_res = self.process(args)
        res = {"instancePath": instance_result,
                "bodyPath":all_shortest_res
                }
        answer = {"code":200,"message":"","data":res}
        return jsonify(answer)
               
class StructureBodyQuery(Resource):

    def __init__(self) -> None:
        self.convert = Converter()
        self.nodes = []
        self.lines = []
        self.root = []
        self.root_list = []
        self.tree_items = []
   
    def _convert_data(self,data):
        #查询结果进行处理，处理成前段需要的格式
        for record in data:
            start_node = record['start']
            relation = record['r']
            end_node = record['end']
            self.convert.add_node(start_node)
            if relation is not None:
                self.convert.add_relation(start_node,relation,end_node)
                self.convert.add_node(end_node)

    #清空查询一次表结构的结果
    def _clear(self):
        self.root = []
        self.nodes = []
        self.lines = []

    #查找虚拟树集合的交集
    def _find_vir_list(self,lists):
        if len(lists) == 0:
            return []
        intersection = set(lists[0] if lists[0] != ['null'] else lists[1])
        for i in range(1, len(lists)):
            if lists[i] != ['null']:
                intersection = intersection.intersection(set(lists[i]))
        return list(intersection)

    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        # parse.add_argument('structureId',required=True,type=str)
        parse.add_argument('structureIdList',required=True,type=str,action="append")
        args = parse.parse_args()
        nodes_list = []
        lines_list = []
        for structureId in args.structureIdList:
            self.convert.clear()
            #查询该表结构下的本体以及关系
            res = q.structure_query(structureId)
            self._convert_data(res)
            self.nodes.append(self.convert.nodes)
            self.lines.append(self.convert.lines)
            #查询表外本体以及关系
            #获得该表结构的根节点nodeId
            str_root  = q.find_str_root(structureId)
            if not str_root: continue
            #获得该表结构下的所有虚拟树集合
            vir_list = q.find_vir_list(structureId)
            vir_list  = self._find_vir_list(vir_list)
            #遍历虚拟树结合找到所有虚拟树的根节点nodeId
            for vir_nodeId in vir_list:
                #每次清除一次表数据
                self.convert.clear()
                vir_root = q.find_vir_root(vir_nodeId)
                vir_name = q.find_vir_name(vir_nodeId)
                #先判断根节点是否在表内，执行表内的where语句。如果不在表内且根节点不同，则说明存在表外结构
                if vir_root == str_root or q.is_inner(vir_root,structureId): continue
                res = q.outer_query(vir_root,str_root)
                self._convert_data(res)
                self.root.append(vir_root)
                #若环形关系存在与Lines中则说明有反向关系
                circle_relation = q.circle_relation(structureId)
                if self.convert.find_line(circle_relation):
                    #弹出对应的虚拟树根节点
                    self.root.pop() 
                    self.convert.clear()
                #若存在说明存在表外数据
                else:
                    self.tree_items.append({"id":vir_nodeId,"virtualTreeName":vir_name}) 
                self.nodes.append(self.convert.nodes)
                self.lines.append(self.convert.lines)
            nodes,lines = unique_node(self.nodes),unique_line(self.lines)
            #生成虚拟的根节点以及关系
            #若存在表外，则生成对应表外虚拟树的虚拟root
            # if self.root!=[]:vir_node,vir_lines = generate_root_data(self.root,structureId)
            # #若不存在表外，则生成对应表结构根节点的虚拟root
            # else:vir_node,vir_lines = generate_root_data([str_root],structureId)
            # nodes.append(vir_node)
            # for line in vir_lines:
            #     lines.append(line)
            # self.root = [vir_node['id']]
            # self.root_list.append(self.root[0])
            self._clear()
            nodes_list.append(nodes)
            lines_list.append(lines)
        nodes,lines = unique_node(nodes_list),unique_line(lines_list)
        # vir_node,vir_lines = generate_root_data(self.root_list,"_List")
        # nodes.append(vir_node)
        # for line in vir_lines:
        #     lines.append(line)
        # res = {
        #         "nodes": nodes, 
        #         "lines": lines,
        #         "virtualTree": self.tree_items,
        #         "rootId":vir_node['id'] 
        #     }
        res = {
                "nodes": nodes, 
                "lines": lines,
                "virtualTree": self.tree_items
            }
        answer = {"code":200,"message":"","data":res}
        return jsonify(answer)

#返回起始节点与给定节点Id列表中的节点是否可达
class AccessList(Resource):

    def __init__(self) -> None:
        pass

    #本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('startNodeId',required=True,type=str)
        parse.add_argument('endNodeIdList',required=True,type=str,action='append')
        args = parse.parse_args()
        #深复制
        res_list = copy.deepcopy(args.endNodeIdList)
        for end_node_id in args.endNodeIdList:
            res = q.hasPath(args.startNodeId,end_node_id)
            if not res: res_list.remove(end_node_id)
        answer = {"code":200,"message":"","data":res_list}
        return jsonify(answer)

#根据本体路径查找子图路径
class FindAllInsPath(Resource):
    def __init__(self) -> None:
        self.convert = Converter()
        self.paths = []

    def _convert_data(self,nodes_map,lines_map):
        #查询结果进行处理，处理成前段需要的格式
        for _,node in nodes_map.items():
            self.convert.add_node(node)
        for tup,line in lines_map.items():
            #拿出startNode和endNode的id
            start_id,end_id = tup.split(',')
            self.convert.add_relation_min_graph(start_id,line,end_id)


    def path_convert(self,data):
        one_start_paths = []
        for record in data:
            nodes_map = {}
            lines_map = {}
            for node in record['nodes']:
                ID = node.__dict__.get('_properties')['nodeId']
                if ID not in nodes_map: nodes_map[ID] = node
            for relation in record['relations']:
                r_dict = relation.__dict__
                start_node = r_dict.get('_start_node')
                inner_start_id =  start_node.__dict__.get('_properties')['nodeId']
                end_node = r_dict.get('_end_node')
                inner_end_id =  end_node.__dict__.get('_properties')['nodeId']
                line_key = inner_start_id+','+inner_end_id
                if line_key in lines_map:continue
                lines_map[line_key] = relation
            #转换成需求格式
            self._convert_data(nodes_map,lines_map)
            path = {"nodes":self.convert.nodes,
                    "lines":self.convert.lines}
            self.paths.append(path)
            self.convert.deep_clear()


    def _convert_data(self,nodes_map,lines_map):
        #查询结果进行处理，处理成前段需要的格式
        for _,node in nodes_map.items():
            self.convert.add_node(node)
        for tup,line in lines_map.items():
            #拿出startNode和endNode的id
            start_id,end_id = tup.split(',')
            self.convert.add_relation_min_graph(start_id,line,end_id)

    #根据查询结果拿到所有instance的列表
    def instances(self,data):
        instances = []
        for node in data:
            node_id = node.data()['ins'].get('nodeId')
            instances.append(node_id)
        return instances

    def process(self,args):
        node_list = args.nodeList
        #查找起始节点的所有instance实例
        start_instance_res = q.get_instance(node_list[0])
        instance_list = self.instances(start_instance_res)
        #拿出每一个start instance
        for start_nodeId in instance_list:
            instance_path = q.instance_path(start_nodeId,node_list[1:],len(node_list)-1)
            self.path_convert(instance_path)

    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('nodeList',required=True,type=str,action="append",help="本体路径上的节点id结合")
        args = parse.parse_args()
        self.process(args)
        answer = {"code":200,"message":"","data":self.paths}
        # answer = {"code":200,"message":"","data":res}
        return jsonify(answer)
        

def unique_line(lines):
    lines = [item for sublist in lines for item in sublist]

    unique_dict = {}
    for item in lines:
        key = (item["from"],item["to"],item["groupId"])
        unique_dict[key] = item

    return list(unique_dict.values())

def unique_node(nodes):
    nodes = [item for sublist in nodes for item in sublist]
    unique_dict = {}
    for item in nodes:
        key = (item["id"])
        unique_dict[key] = item

    return list(unique_dict.values())

def  generate_root_data(id_list,name=None):
    root_id = generate_id()
    vir_node = {}
    vir_node["id"] = root_id 
    vir_node["info"] ={
            "defaultColor": "RGBA(255, 255, 255, 1)",
            "fileType": None,
            "remark": "备注信息",
            "snType": None,
            "type": None}
    vir_node["text"] = "root"+ (name if name else '')
    vir_lines = []
    for id in id_list:
        line_dict = {}
        line_dict["id"] = generate_id()
        line_dict["from"] = id
        line_dict["to"] = root_id
        line_dict["groupId"] = None
        line_dict["text"] =  None
        line_dict["info"] = {"relationType":  None,
                             "treeId":  None,
                             "labelList": "未导入部分"}
        vir_lines.append(line_dict)
    return vir_node,vir_lines