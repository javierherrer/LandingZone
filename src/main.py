from api_zip_hdfs import download_unemployment
from move_files_from_resources import zip_and_move_folder
from data_collector import execute_data_collector
from persistent_collector import persistent_collector
from clean_temporal_zone import clean_temporal_zone

import argparse

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

    return parser



def pipeline(args):
    download_unemployment()
    zip_and_move_folder('../resources', 'data', 'data.zip')
    execute_data_collector(args)
    persistent_collector(args)
    clean_temporal_zone(args)

if __name__ == '__main__':
    ## Extract arguments
    args = get_parser().parse_args()

    pipeline(args)