from flask import jsonify,make_response
from neo4j_query.package_query import PackageQuery
from flask_restful import Resource,reqparse
from api.utils import *
from api.convert import Converter
# from RDBS.db_utils import Mysql
pq = PackageQuery()

class FilesQuery(Resource):
    def __init__(self) -> None:
        self.convert = Converter()
        self.items = []
        self.res = {}
    
    def process(self,args):
        res = {}
        for nodeId in args.nodeList:
            query_res = pq.find_file(nodeId)
            files = [file.data().get('nodes')[2].get('nodeId') for file in query_res]
            res[nodeId] = files
        return res

    # 本体/实体个数统计查询,若未指定类型返回总节点数
    def post(self):
        # req_data = request.get_json(force=True)
        parse = reqparse.RequestParser()
        parse.add_argument('nodeList',required=True,type=str,action="append")
        args = parse.parse_args()
        res = self.process(args)
        # self._convert_data(res)
        answer = {"code": 200, "message": "", "data": res}
        return jsonify(answer)