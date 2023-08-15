from flask import request,jsonify
from neo4j_query.query import Query


class QueryApi():
    def __init__(self) -> None:
        self.q = Query()

    #检查参数
    def check_args(self):
        pass


     # 检查参数
    def convert_args(self,args):
       args['label'] = ['body','instance'] if args['label'] is None or args['label'] == '' else args['label']
       return args
    #本体/实体个数统计查询,若未指定类型返回总节点数
    
    def countQuery(self):
        req_data = request.get_json(force=True)
        args = self.convert_args(req_data)
        res = self.q.count_query(args['label'])
        answer = {"code":200,"message":"","data":res}
        return jsonify(answer)

    #一跳关系查询
    def oneHopQuery(self):
        req_data = request.get_json(force=True)
        