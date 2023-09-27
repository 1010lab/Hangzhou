from flask import request,jsonify
from NEO4J_data_import.main import *
from flask_restful import Resource,reqparse
import os
from dotenv import load_dotenv

path = os.path.join(os.getcwd(),'.env')
load_dotenv(path)
NEO4J_PATH = os.environ.get("NEO4J_PATH")

'''
    args = {} ->
    args["site_id"] : 站点名称
    args["neo4j_path"]:neo4j路径
    args["data_path"] : 接收站点数据时所存储的文件路径 默认为data
    args["neo4j_data_path"]:生成neo4j数据的本文件下的文件路径 默认为neo4j_data
    check_args(req_data) : 检查所有参数是否有值
'''
class LoadApi(Resource):
    def __init__(self) -> None:
        pass

    def convert_args(self,args):
        args["NEO4J_PATH"] = NEO4J_PATH
        args["DATA_PATH"] = args["dataPath"]
        args["NEO4J_DATA_PATH"] = args["neo4jDataPath"]
        return args

    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument("siteID",type=str,required=True,help="站点名称不能为空")
        parse.add_argument("neo4jPath",type=str,required=True,help="neo4j目录不能为空")
        parse.add_argument("dataPath",type=str,default=r"NEO4J_data_import/data")
        parse.add_argument("neo4jDataPath",type=str,default=r"NEO4J_data_import/neo4j_data")
        args = parse.parse_args()
        args = self.convert_args(args)
        r = main(args)
        # 拼接code，message，data
        answer = {"code":200,"message":"","data":r}
        return jsonify(answer)