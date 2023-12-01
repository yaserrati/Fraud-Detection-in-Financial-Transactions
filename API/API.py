from flask import Flask, jsonify
import pandas as pd
import json

app = Flask(__name__)

def get_data(file_path):
    try:
        with open(file_path, 'r') as file:
            if file:
                return json.load(file)
            else:
                return jsonify({'error': 'Data not found'}), 404

    except FileNotFoundError:
        return jsonify({'error': 'File not found'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_dataframe_and_response(file_path):
    df = pd.read_json(file_path, orient='records')
    item = df.to_json(orient='records')
    
    if item:
        return json.loads(item)
    else:
        return jsonify({'error': 'Data not found'}), 404

# "get all transactions. (transactions endpoint)"
@app.route("/api/transactions/", methods=['GET'])
def get_all_transactions():
    return get_dataframe_and_response("../data/transactions.json")

# "get all customers. (customers endpoint)"
@app.route("/api/customers/", methods=['GET'])
def get_all_customers():
    return get_dataframe_and_response("../data/customers.json")

# "get all external_data. (external_data endpoint)"
@app.route("/api/external_data/", methods=['GET'])
def get_all_external_data():
    return get_data('../data/external_data.json')
