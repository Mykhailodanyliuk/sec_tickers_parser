import datetime
import json
import time

import pymongo
import requests


def get_collection_from_db(data_base, collection, client):
    db = client[data_base]
    return db[collection]


def upload_sec_tickers_data():
    update_collection = get_collection_from_db('db', 'update_collection', client)
    sec_tickers_data_collection = get_collection_from_db('db', 'sec_data_tickers', client)
    last_len_records = sec_tickers_data_collection.estimated_document_count()
    while True:
        try:
            loc_json = json.loads((requests.get('https://www.sec.gov/files/company_tickers.json')).text)
            break
        except:
            time.sleep(60)
            pass
    for company in loc_json:
        cik_str = str(loc_json[company].get('cik_str')).zfill(10)
        print(cik_str)
        ticker = loc_json[company].get('ticker')
        title = loc_json[company].get('title')
        if sec_tickers_data_collection.find_one({'cik_str': str(loc_json[company].get('cik_str')).zfill(10)}) is None:
            sec_tickers_data_collection.insert_one(
                {'cik_str': cik_str, 'tickers': [ticker], 'title': title, 'upload_at': datetime.datetime.now()})
        else:
            tickers_db = sec_tickers_data_collection.find_one(
                {'cik_str': str(loc_json[company].get('cik_str')).zfill(10)}).get('tickers')
            if ticker not in tickers_db:
                tickers_db.append(ticker)
            update_query = {'tickers': tickers_db}
            sec_tickers_data_collection.update_one({'cik_str': str(loc_json[company].get('cik_str')).zfill(10)},
                                                   {"$set": update_query})

    total_records = sec_tickers_data_collection.estimated_document_count()
    update_query = {'name': 'sec_tickers', 'new_records': total_records - last_len_records,
                    'total_records': total_records,
                    'update_date': datetime.datetime.now()}
    if update_collection.find_one({'name': 'sec_tickers'}):
        update_collection.update_one({'name': 'sec_tickers'}, {"$set": update_query})
    else:
        update_collection.insert_one(update_query)


if __name__ == '__main__':
    while True:
        client = pymongo.MongoClient('mongodb://localhost:27017')
        upload_sec_tickers_data()
        client.close()
