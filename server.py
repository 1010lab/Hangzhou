from concurrent.futures import thread
from flask import Flask, request, render_template,jsonify
# from flask_apscheduler import APScheduler

from flask_cors import CORS
# from gevent import pywsgi

#获取具体的服务
from neo4j_query.query import Query
from NEO4J_data_import.main import *

# 参数
import argparse
    
app=Flask(__name__)

CORS(app, resources=r'/*',supports_credentials=True)

q = Query()


#检查参数
def check_args(args):
    assert args["siteID"] is not None,"站点名称不能为空"
    assert args["neo4jPath"] is not None,"neo4j目录不能为空"
    if args.get("dataPath") == None:
        args["dataPath"] = r"NEO4J_data_import/data"
        # raise Exception("接收站点数据时所存储的文件路径不能为空")
    if args.get("neo4jDataPath") == None:
        args["neo4jDataPath"] = r"NEO4J_data_import/neo4j_data"
        # raise Exception("生成neo4j数据的本文件下的文件路径不能为空")
    return args

def convert_args(args):
    args["NEO4J_PATH"] = args["neo4jPath"]
    args["DATA_PATH"] = args["dataPath"]
    args["NEO4J_DATA_PATH"] = args["neo4jDataPath"]
    return args

@app.route('/loadNodeAndRelationToNeo4j', methods=['POST'])
def loadNodeAndRelationToNeo4j():
    req_data = request.get_json(force=True)
    # 所有的参数
    # args = {}
    # # 站点名称
    # args["site_id"] = req_data["siteID"]
    # # neo4j路径
    # args["neo4j_path"] = req_data["neo4jPath"]
    # # 接收站点数据时所存储的文件路径 默认为data
    # args["data_path"] = req_data["dataPath"]
    # # 生成neo4j数据的本文件下的文件路径 默认为neo4j_data
    # args["neo4j_data_path"] = req_data["neo4jDataPath"]
    # 检查所有参数是否有值
    args = check_args(req_data)
    args = convert_args(args)
    print(args["DATA_PATH"])
    r = main(args)
    res = {'node_num':r.node_num,'relation_num':r.relation_num,
           'node_info':r.node_info,'relation_info':r.relation_info}
    # 拼接code，message，data
    answer = {"code":200,"message":"","data":res}
    return jsonify(answer)

@app.route('query/countQuery', methods=['POST'])
def countQuery():
    req_data = request.get_json(force=True)
    # 所有的参数
    # label
    # # 查询结点的标签值
    args = check_args(req_data)
    args = convert_args(args)
    print(args["DATA_PATH"])
    r = main(args)
    res = {'node_num':r.node_num,'relation_num':r.relation_num,
           'node_info':r.node_info,'relation_info':r.relation_info}
    # 拼接code，message，data
    answer = {"code":200,"message":"","data":res}
    
 
if __name__=='__main__':
    app.run(host="0.0.0.0",threaded=True)
    # server.serve_forever()
