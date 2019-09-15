from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from pymongo import MongoClient
from bson.json_util import dumps
import json

app = Flask(__name__)
api = Api(app)

client = MongoClient("mongodb://db:27017")
db = client.stocks

class TotalStocks(Resource):
    def get(self):
        total = dumps(db.mojoanalysis.find({}))
        total = json.loads(total)
        return total

class MidCaps(Resource):
    def get(self):
        mid_stocks = dumps(db.mojoanalysis.find({"$text":{"$search": "\"Mid Cap\"" }}))
        mid_stocks = json.loads(mid_stocks)
        count = len(mid_stocks)
        ret_map = {
            "total":count,
            "stocks": mid_stocks
        }
        return jsonify(ret_map)

class StockInfo(Resource):
    def post(self):
        posted_data = request.get_json()
        stock_name = posted_data["stock"]
        stock_name = '"{}"'.format(stock_name)
        stocks_data = json.loads(dumps(db.mojoanalysis.find({ "$text": { "$search": stock_name}})))
        return stocks_data

class QualityFilter(Resource):
    def post(self):
        posted_data = request.get_json()
        quality_1 = posted_data['quality_1']
        quality_2 = posted_data['quality_2']
        stocks = dumps(db.mojoanalysis.aggregate(
        [
        { "$match": { "$or": [ { "quality": quality_1},
        { "quality": quality_2} ] } } ])
        )
        stocks = json.loads(stocks)
        count = dumps(db.mojoanalysis.aggregate(
        [
        { "$match": 
        { "$or": [ { "quality": quality_1 }, { "quality": quality_2} ] } },
        { "$group": { "_id": None, "count": { "$sum": 1 } } }])
        )
        count = json.loads(count)
        ret_map = {
                "total":count,
                "detailed":stocks
        }
        return jsonify(ret_map)

api.add_resource(TotalStocks, "/total")
api.add_resource(MidCaps, "/mid_caps")
api.add_resource(StockInfo,"/stock_info")
api.add_resource(QualityFilter,"/quality_filter")

if __name__=="__main__":
    app.run(host="0.0.0.0", debug=True)
