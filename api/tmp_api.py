from flask import jsonify
from flask_restful import Resource

class Hello(Resource):
    def get(self):
        return jsonify("hello world")