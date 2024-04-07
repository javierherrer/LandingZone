from vm_connection import VM
from hdfs import InsecureClient
import argparse
from pymongo import MongoClient
import json
import pickle as pkl
import os

import pandas as pd

from cli_parser import get_parser

args = get_parser().parse_args()

def connect_to_vm():
    vm = VM()
    return vm

def initialize_hdfs(hostname, port, user):
    hdfs_client = InsecureClient('http://{}:{}/'.format(hostname, port), user=user)
    return hdfs_client

def initialize_mongodb(hostname, port, name_db, collection_name):
    # Connect to MongoDB using a docker container
    client = MongoClient(host=hostname, port=port, connect=True) 

    # Access database
    db = client[name_db]
    
    ## Access collection name
    collection = db[collection_name]

    return collection


def load_json(hdfs_client, hdfs_file_path, collection, set_of_files):
    with hdfs_client.read(hdfs_file_path, encoding='utf-8') as reader:
        # Read CSV file into a DataFrame, ideally it will be made by spark in order to avoid bottlenecks
        json_data = json.load(reader)
    if collection.name == 'unemployment-data': 
        json_data = dict(json_data)['result']['records']
    # Insert data into MongoDB collection
    try: 
        collection.insert_many(json_data)
        set_of_files.add(hdfs_file_path)
    except TypeError as e: 
        print(e)



def load_csv(hdfs_client, hdfs_file_path, collection, set_of_files):
    # Read CSV file into a DataFrame, ideally it will be made by spark in order to avoid bottlenecks
    with hdfs_client.read(hdfs_file_path, encoding='utf-8') as reader:
        df = pd.read_csv(reader)
    
    # Convert DataFrame to a list of dictionaries (JSON-like format)
    csv_data = df.to_dict(orient='records')
    
    # Insert data into MongoDB collection
    try: 
        collection.insert_many(csv_data)
        set_of_files.add(hdfs_file_path)
    except TypeError as e: 
        print(e)



def load_tracking_files(directory="tracking_data", file="tracking_data.pkl"):
    try: 
        with open(f"{directory}/{file}", 'rb') as f:
            set_of_files = pkl.load(f)
    except FileNotFoundError: 
        with open(f"{directory}/{file}", 'wb') as f:
            set_of_files = set()
            pkl.dump(set_of_files, f)

    return set_of_files

def update_tracking_files(set_of_files, directory="tracking_data", file="tracking_data.pkl"):
    with open(f"{directory}/{file}", 'wb') as f:
        pkl.dump(set_of_files, f)

# Example usage:


def persistent_collector(args):
    ## Connect to VM
    vm = VM(hostname=args.host_ip, port=22, username=args.vm_user, password=args.vm_pass)
    
    print('----------------------------------------------------------')
    print('Starting MongoDB on Virtual Machine...')
    command = '~/BDM_Software/mongodb/bin/mongod --bind_ip_all --dbpath /home/bdm/BDM_Software/data/mongodb_data/ --fork --logpath /home/bdm/BDM_Software/data/mongodb_data/mongod.log'
    output = vm.exe(command)
    print(output)

    ## Load hdfs client
    hdfs_client = initialize_hdfs(hostname=vm.hostname, port=args.hdfs_port, user=vm.username)

    os.makedirs("tracking_data", exist_ok=True)
    set_of_files = load_tracking_files()


    for src in hdfs_client.list(vm._DIR_TEMPORAL):
        if src in ['idealista', 'unemployment-data']: json_ = True
        else: json_ = False
        ## Initialize mongo collection
        mongo_collection = initialize_mongodb(hostname=vm.hostname, port=args.mongo_port, name_db=args.name_mongo_db,  collection_name=src)
        for file in hdfs_client.list(f'{vm._DIR_TEMPORAL}/{src}'):
            filepath = f'{vm._DIR_TEMPORAL}/{src}/{file}'
            if filepath in set_of_files: pass
            else:
                print(filepath)
                if json_: 
                    load_json(hdfs_client=hdfs_client, hdfs_file_path=filepath, collection=mongo_collection, set_of_files=set_of_files)
                else: 
                    load_csv(hdfs_client=hdfs_client, hdfs_file_path=filepath, collection=mongo_collection, set_of_files=set_of_files)

        update_tracking_files(set_of_files=set_of_files)

    return True

if __name__ == '__main__':
    persistent_collector(args=args)

