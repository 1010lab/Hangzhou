from flask import request,jsonify
from NEO4J_data_import.main import *


'''
    所有的参数
    args = {} ->
    args["site_id"] : 站点名称
    args["neo4j_path"]:neo4j路径
    args["data_path"] : 接收站点数据时所存储的文件路径 默认为data
    args["neo4j_data_path"]:生成neo4j数据的本文件下的文件路径 默认为neo4j_data
    check_args(req_data) : 检查所有参数是否有值
'''
class LoadApi():
    def __init__(self) -> None:
        pass


    def convert_args(self,args):
        args["NEO4J_PATH"] = args["neo4jPath"]
        args["DATA_PATH"] = args["dataPath"]
        args["NEO4J_DATA_PATH"] = args["neo4jDataPath"]
        return args

    
    # 检查参数
    def check_args(self,args):
        assert args["siteID"] is not None,"站点名称不能为空"
        assert args["neo4jPath"] is not None,"neo4j目录不能为空"
        if args.get("dataPath") == None:
            args["dataPath"] = r"NEO4J_data_import/data"
            # raise Exception("接收站点数据时所存储的文件路径不能为空")
        if args.get("neo4jDataPath") == None:
            args["neo4jDataPath"] = r"NEO4J_data_import/neo4j_data"
            # raise Exception("生成neo4j数据的本文件下的文件路径不能为空")
        return args

    def loadNodeAndRelationToNeo4j(self):
        req_data = request.get_json(force=True)
        args = self.check_args(req_data)
        args = self.convert_args(args)

        r = main(args)
        res = {'node_num':r.node_num,'relation_num':r.relation_num,
            'node_info':r.node_info,'relation_info':r.relation_info}
        # 拼接code，message，data
        answer = {"code":200,"message":"","data":res}
        return jsonify(answer)