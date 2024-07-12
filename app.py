from flask import Flask, request, jsonify
import pandas as pd
from pymongo import MongoClient
from datetime import datetime  
import json

app = Flask(__name__)

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/')
db = client['your_database']
collections = db['colors_hsm']

# testing dictionary for testing
channel_data_dict = {}

@app.route('/import', methods=['POST'])
def import_data():
    try:
        
        df = pd.read_excel(r'D:\gapsmith\excel_read\channels.xlsx')

        colors_hsm_channels = df['Channel'].unique()

        colors_hsm_df = df[df['Channel'].isin(colors_hsm_channels)]

        for index, row in colors_hsm_df.iterrows():
            channel_name = row['Channel']

           
            channel_data = {
                'Date': row['Date'].isoformat() if isinstance(row['Date'], datetime) else None,
                'Advertiser': row['Advertiser'],
                'AMD Agency': row['AMD Agency'],
                'Brand Name': row['Brand Name'],
                'Rate': row['Rate'],
                'Unit Rate': row['Unit Rate'],
                'Length': row['Length'],
                'Title': row['Title'],
                'House Number': row['House Number'],
                'Reference #': row['Reference #'],
                'Parent RO #': row['Parent RO #'],
            }

            if 'Name' in row:
                names = row['Name'].split('/')
                channel_data['Type'] = names[0].strip() if len(names) > 0 else ''
                channel_data['Days'] = names[1].strip() if len(names) > 1 else ''
                channel_data['Number'] = names[2].strip() if len(names) > 2 else ''

            if channel_name not in channel_data_dict:
                channel_data_dict[channel_name] = []

            channel_data_dict[channel_name].append(channel_data)

        

        for channel, data_list in channel_data_dict.items():
            for data in data_list:
                print(json.dumps(data))  

            return jsonify({'status': 'Data imported successfully!'})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/data', methods=['GET'])
def get_data():

    return jsonify(channel_data_dict)

if __name__ == '__main__':
    app.run(debug=True)
