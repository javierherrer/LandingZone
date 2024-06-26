import datetime
from vm_connection import VM
from file_operations import (transfer_source_files, delete_file_in_dir, unzip_source_files,
                             get_files_in_folder, check_if_hdfs_dir_exists, make_hdfs_dir,
                             delete_dir, move_file_to_hdfs, query_composer, move_to_hfds_query, make_dir)
from cli_parser import get_parser

args = get_parser().parse_args()

def execute_data_collector(args):
    print("Uploading files")
    vm = VM(hostname=args.host_ip, port=22, username=args.vm_user, password=args.vm_pass)
    date = datetime.date.today()
    make_dir(vm=vm, dir="data")

    print("_" * 100)
    print("Transferring files...")
    print("")
    transfer_source_files(vm, "data/data.zip", "/home/bdm/data/data.zip")

    print("Unzipping files...")
    print("")
    unzip_source_files(vm)

    print("Deleting zip file...")
    print("")
    delete_file_in_dir(vm, '/home/bdm/data/data.zip')

    files = get_files_in_folder(vm, '/home/bdm/data')
    print("FIles are: ", files)

    if not check_if_hdfs_dir_exists(vm, f'temporary'):
        make_hdfs_dir(vm, f'temporary')
    for file in files.split('\n'):
        if file != "":
            print("_" * 100)
            print(f"Checking if /temporary/{file} exists...")
            if not check_if_hdfs_dir_exists(vm, f'temporary/{file}'):
                print(f"/temporary/{file} does not exist. Creating the dir...")
                make_hdfs_dir(vm, f'temporary/{file}')
                print("Success")
                print("")
            else:
                print(f"/temporary/{file} exists")
                print("")

            print("."*100)
            print(f"Checking if /temporary/{file} exists...")
            if not check_if_hdfs_dir_exists(vm, f'temporary/{file}'):
                print(f"/temporary/{file} does not exist. Creating the dir...")
                make_hdfs_dir(vm, f'temporary/{file}')
                print("")
            else:
                print(f"/temporary/{file} exists")
                print("")

            print("." * 100)
            print(f"Loading files from {file} to hdfs dir: {vm._DIR_TEMPORAL}/{file}...")

            fs = get_files_in_folder(vm, f'/home/bdm/data/{file}')
            hdfs_query = ""
            delete_query = ""
            for f in fs.split('\n'):
                if f != "":
                    hdfs_query = query_composer(
                        hdfs_query, 
                        move_to_hfds_query(f'/home/bdm/data/{file}/{f}', f'{vm._DIR_TEMPORAL}/{file}')
                    )
                    delete_query = query_composer(delete_query, f'rm -rf /home/bdm/data')

            vm.exe(hdfs_query)
            print("All files moved successfully!")
            print("")

            print(f"All files from {file} were successfully processed")
            print("_"*100)

        vm.exe(delete_query)
        delete_dir(vm, f'/home/bdm/data')

if __name__ == '__main__':
    execute_data_collector(args=args)