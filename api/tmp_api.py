from flask import jsonify,make_response
from neo4j_query.query import Query
from flask_restful import Resource,reqparse

class Hello(Resource):
    def get(self):
        return jsonify("hello world")