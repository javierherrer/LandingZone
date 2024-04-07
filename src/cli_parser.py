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
