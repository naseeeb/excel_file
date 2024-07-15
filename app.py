from flask import Flask, request, jsonify
import pandas as pd
from pymongo import MongoClient
from datetime import datetime  
import json

app = Flask(__name__)

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/')
db = client['exceldb']
colors_hsm_collection = db['colors_hsm']
zee_network_collection = db['zee_network']
star_network_collection = db['star_network']

@app.route('/import_hsm', methods=['POST'])
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

            colors_hsm_collection.update_one(
                {'Channel': channel_name},
                {'$push': {'data': channel_data}},
                upsert=True
            )

        return jsonify({'status': 'Data imported successfully!'})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/data_hsm_channel', methods=['GET'])
def get_data():
    data = list(colors_hsm_collection.find({}, {'_id': 0}))
    return jsonify(data)


@app.route('/import_zee_network', methods=['POST'])
def import_zee():
    try:
        df = pd.read_excel(r'D:\gapsmith\excel_read\zee_network.xlsx')
        zee_network_channels = df['ChannelName'].unique()
        zee_network_df = df[df['ChannelName'].isin(zee_network_channels)]

        for index, row in zee_network_df.iterrows():
            channel_name = row['ChannelName']

            channel_data = {
                'BookingReferenceNumber': row['BookingReferenceNumber'],
                'ClientName': row['ClientName'],
                'AgencyName': row['AgencyName'],
                'ProgramName': row['ProgramName'],
                'ScheduleDate': row['ScheduleDate'].isoformat() if isinstance(row['ScheduleDate'], datetime) else None,
                'TAPEID': row['TAPEID'],
                'CommercialCaption': row['CommercialCaption'],
                'TapeDuration': row['TapeDuration'],
                'SpotAmount': row['SpotAmount'],
                'BrandName': row['BrandName'],
                'SpotStatus': row['SpotStatus'],
                'Recordnumber': row['Recordnumber'],
                'Starttime': row['Starttime'].isoformat() if isinstance(row['Starttime'], datetime) else None,
                'Endtime': row['EndTime'].isoformat() if isinstance(row['EndTime'], datetime) else None,
                'SponsorTypeName': row['SponsorTypeName'],
                'Accountname': row['Accountname'],
                'ScheduledProgram': row['ScheduledProgram'],
                'DealTypeName': row['DealTypeName'],
                'DayofTheWeek': row['DayofTheWeek'],
                'spottype': row['spottype'],
                'Personnelname': row['Personnelname'],
                'ScheduleTime': row['ScheduleTime'].isoformat() if isinstance(row['ScheduleTime'], datetime) else None,
            }

            zee_network_collection.update_one(
                {'ChannelName': channel_name},
                {'$push': {'data': channel_data}},
                upsert=True
            )

        return jsonify({'status': 'Data imported successfully!'})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/data_zee_network', methods=['GET'])
def get_zee():
    data = list(zee_network_collection.find({}, {'_id': 0}))
    return jsonify(data)

standard_schema_mapping = {
    "telecast_date": ["Telecast Date", "Broadcast Date"],
    "advertiser": ["Advertiser", "Client"],
    "agency": ["Agency", "Agency Name"],
    "gstin_number_buyer": ["GSTIN Number (BUYER)", "GSTIN"],
    "program": ["Program", "Show"],
    "sales_unit": ["Sales Unit"],
    "aired_time": ["Aired Time", "Broadcast Time"],
    "duration_seconds": ["Duration (Seconds)", "Duration"],
    "pitched_price": ["Pitched Price", "Quoted Price"],
    "plan": ["Plan"],
    "plan_number": ["Plan Number"],
    "deal_number": ["Deal Number"],
    "sales_team": ["Sales Team"],
    "sales_executive": ["Sales Executive"],
    "sales_location": ["Sales Location"],
    "entity_state": ["Entity State"],
    "entity_gstin_number": ["Entity GSTIN Number", "GSTIN Entity"],
    "currency": ["Currency"],
    "reference_number": ["Reference Number"],
    "dy": ["DY"],
    "brand": ["Brand"],
    "invoice_no": ["INVOICE_NO", "Invoice Number"],
    "commercial_material": ["Commercial Material", "Ad Material"],
    "hsn_code": ["HSN_CODE*", "HSN Code"],
    "ro_number": ["RO_NUMBER", "RO Number"]
}

def normalize_row(row, mapping):
    normalized_data = {}
    for standard_field, original_fields in mapping.items():
        for original_field in original_fields:
            if original_field in row:
                normalized_data[standard_field] = row[original_field]
                break
    return normalized_data


@app.route('/import_star_network', methods=['POST'])
def import_star_network():
    try:
        df = pd.read_excel(r'D:\gapsmith\excel_read\star_network.xlsx')
        star_network_channels = df['Channel Name'].unique()
        star_network_df = df[df['Channel Name'].isin(star_network_channels)]

        for index, row in star_network_df.iterrows():
            channel_name = row['Channel Name']
            normalized_data = normalize_row(row, standard_schema_mapping)

            star_network_collection.update_one(
                {'ChannelName': channel_name},
                {'$push': {'data': normalized_data}},
                upsert=True
            )

        return jsonify({'status': 'Data imported successfully!'})

    except Exception as e:
        return jsonify({'error': str(e)}), 500@app.route('/data_star_network', methods=['GET'])
@app.route('/data_star_network', methods=['GET'])
def get_star_data():
    data = list(star_network_collection.find({}, {'_id': 0}))
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
