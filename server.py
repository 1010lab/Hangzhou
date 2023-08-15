from concurrent.futures import thread
from flask import Flask, request, render_template,jsonify
# from flask_apscheduler import APScheduler
from flask_restful import Api
from flask_cors import CORS
# from gevent import pywsgi
#获取具体的服务

from api.load_api import LoadApi
from api.query_api import *

app=Flask(__name__)

CORS(app, resources=r'/*',supports_credentials=True)

api = Api(app)

# load_api =  LoadApi()
# app.add_url_rule('/loadNodeAndRelationToNeo4j', view_func=load_api.loadNodeAndRelationToNeo4j,methods=['POST'])

# query_api = QueryApi()
# app.add_url_rule(r'/query/countQuery', view_func=query_api.countQuery,methods=['POST'])
api.add_resource(LoadApi,'/loadNodeAndRelationToNeo4j')
api.add_resource(CountQuery,'/query/countQuery')
api.add_resource(OneHopQuery,'/query/oneHopQuery')
api.add_resource(ThreeHopQuery,'/query/threeHopQuery')
api.add_resource(ByAttributeQuery,'/query/byAttributeQuery')

if __name__=='__main__':
    app.run(host="0.0.0.0",threaded=True)
    # server.serve_forever()
