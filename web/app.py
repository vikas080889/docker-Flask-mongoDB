from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from pymongo import MongoClient
from bson.json_util import dumps
import json

app = Flask(__name__)
api = Api(app)

client = MongoClient("mongodb://db:27017")
db = client.stocks
db2 = client.bseStocks

class TotalStocks(Resource):
    def get(self):
        total = dumps(db.mojoanalysis.find({}))
        total = json.loads(total)
        return total

class MidCaps(Resource):
    def get(self):
        mid_stocks = dumps(db.mojoanalysis.find({"$text":
        {"$search": "\"Mid Cap\"" }}))
        mid_stocks = json.loads(mid_stocks)
        count = len(mid_stocks)
        ret_map = {
            "total":count,
            "stocks": mid_stocks
        }
        return jsonify(ret_map)
# Informatic of Indvidual stock
class StockInfo(Resource):
    def post(self):
        posted_data = request.get_json()
        stock_name = posted_data["stock"]
        stock_name = '"{}"'.format(stock_name)
        stocks_data = json.loads(dumps(db.mojoanalysis.find({ "$text":
        { "$search": stock_name}})))
        return stocks_data

class QualityFilter(Resource):
    def post(self):
        posted_data = request.get_json()
        segment = posted_data['segment']
        quality = posted_data['quality']
        # Filtered stocks with quality
        stocks = dumps(db.mojoanalysis.aggregate(
        [
        { "$match": { "$and": [ { "market_cap": segment},
        { "quality": quality} ] } } ])
        )
        stocks = json.loads(stocks)
        # count of filtered stocks with quality
        count = dumps(db.mojoanalysis.aggregate(
        [
        { "$match":
        #instead of $and $or can be used according to scenarios
        { "$and": [ { "market_cap": segment }, { "quality": quality} ] } },
        { "$group": { "_id": None, "count": { "$sum": 1 } } }])
        )
        count = json.loads(count)
        ret_map = {
                "total":count,
                "detailed":stocks
        }
        return jsonify(ret_map)
# Get selected fields from filtered stocks
class QualityFilterSelectedField(Resource):
      def post(self):
          posted_data = request.get_json()
          segment = posted_data['segment']
          quality = posted_data['quality']
          stocks = dumps(db.mojoanalysis.aggregate( [{ "$match": { "$and":
                [ { "market_cap": segment }, { "quality": quality}]}},
                { "$project" :
                 { "_id": 1,"valuation":1,"quality":1,"URL":1,"market_cap":1} }
                 ]))
          stocks = json.loads(stocks)
          return stocks

class StocksFromKyc(Resource):
      def post(self):
          posted_data = request.get_json()
          kyc_data = posted_data['kyc_data']
           #quality = posted_data['quality']
          stocks = dumps(db.mojoanalysis.find(
                      {"$text": {"$search": kyc_data}},{"URL":1}))
          stocks = json.loads(stocks)
          return stocks

class BSESustainedProfitGain(Resource):
    def get(self):
        stocks = dumps(db2.stockreposts.aggregate([{"$match":
        {"$and" :[{"$expr":{"$gt":["$q1_profit", "$q2_profit"]}},
        {"$expr":{"$gt":["$q2_profit", "$q3_profit"]}},
        {"$expr":{"$gt":["$q3_profit", "$q4_profit"]}}  ]}}]))
        stocks = json.loads(stocks)
        count = len(stocks)
        ret_map = {
            "total":count,
            "stocks": stocks
        }
        return jsonify(ret_map)


api.add_resource(TotalStocks, "/total")
api.add_resource(MidCaps, "/mid_caps")
api.add_resource(StockInfo,"/stock_info")
api.add_resource(QualityFilter,"/quality_filter")
api.add_resource(QualityFilterSelectedField,"/quality_filter_selected")
api.add_resource(StocksFromKyc,"/kyc_filter")
api.add_resource(BSESustainedProfitGain,"/bse_sustained_profit_gain")

if __name__=="__main__":
    app.run(host="0.0.0.0", debug=True)
