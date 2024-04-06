from vm_connection import VM
from hdfs import InsecureClient
import argparse
from pymongo import MongoClient
import json
import pickle as pkl
import os


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--host_ip",
                        help="host ip of virtual machine / mongo server",
                        type=str, default = '10.4.41.51')
    parser.add_argument("-d", "--hdfs_port",
                        help="hdfs port of virtual machine",
                        type=int, default = 9870)
    parser.add_argument("-m", "--mongo_port",
                        help="mongo port of mongodb instance in virtual machine",
                        type=int, default = 27017)                    
    parser.add_argument("-u", "--vm_user",
                        help="username to log into virtual machine",
                        type=str, default = 'bdm')
    parser.add_argument("-p", "--vm_pass",
                        help="password to log into virtual machine",
                        type=str, default = 'bdm')
    parser.add_argument("-n", "--name_mongo_db", 
                        help="Name of the mongo database",
                        type=str, default='P1')
    parser.add_argument("-a", "--allow_metadata",
                        help="Wheather or not to work with a seperate metadata file keeping track of which data has already been added to mongoDB.",
                        type=str, default='True')         
    return parser

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


def load_json(hdfs_client, hdfs_file_path, collection):
    with hdfs_client.read(hdfs_file_path, encoding='utf-8') as reader:
        json_data = json.load(reader)
        # Insert data into MongoDB collection
        try: collection.insert_many(json_data)
        except TypeError as e: print(e)



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


if __name__ == '__main__':
    ## Extract arguments
    args = get_parser().parse_args()
    
    ## Connect to VM
    vm = VM()
    
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
        if src in ['idealista', 'unemployment-data']:
            ## Initialize mongo collection
            mongo_collection = initialize_mongodb(hostname=vm.hostname, port=args.mongo_port, name_db=args.name_mongo_db,  collection_name=src)
            
            for dt in hdfs_client.list(f'{vm._DIR_TEMPORAL}/{src}'):
                for file in hdfs_client.list(f'{vm._DIR_TEMPORAL}/{src}/{dt}'):
                    filepath = f'{vm._DIR_TEMPORAL}/{src}/{dt}/{file}'
                    if filepath in set_of_files: pass
                    else:
                        print(filepath)
                        load_json(hdfs_client=hdfs_client, hdfs_file_path=filepath, collection=mongo_collection)
                        set_of_files.add(filepath)

    update_tracking_files(set_of_files=set_of_files)
    

