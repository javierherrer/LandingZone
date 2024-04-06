import datetime
from vm_connection import VM
from file_operations import (transfer_source_files, delete_file_in_dir, unzip_source_files,
                             get_files_in_folder, check_if_hdfs_dir_exists, make_hdfs_dir,
                             delete_dir, move_file_to_hdfs, query_composer, move_to_hfds_query)

if __name__ == "__main__":
    vm = VM()
    date = datetime.date.today()
    print("_" * 100)
    print("Transferring files...")
    print("")
    transfer_source_files(vm, "C:\\Users\\Admin\\Desktop\\MASTER\\Q2\\BDM\\LAB\\data\\data.zip", "/home/bdm/data/data.zip")

    print("Unzipping files...")
    print("")
    unzip_source_files(vm)

    print("Deleting zip file...")
    print("")
    delete_file_in_dir(vm, '/home/bdm/data/data.zip')

    files = get_files_in_folder(vm, '/home/bdm/data')
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
            print(f"Checking if /temporary/{file}/{date} exists...")
            if not check_if_hdfs_dir_exists(vm, f'temporary/{file}/{date}'):
                print(f"/temporary/{file}/{date} does not exist. Creating the dir...")
                make_hdfs_dir(vm, f'temporary/{file}/{date}')
                print("")
            else:
                print(f"/temporary/{file}/{date} exists")
                print("")

            print("." * 100)
            print(f"Loading files from {file} to hdfs dir: /user/temporary/{file}/{date}...")

            fs = get_files_in_folder(vm, f'/home/bdm/data/{file}')
            hdfs_query = ""
            delete_query = ""
            for f in fs.split('\n'):
                if f != "":
                    hdfs_query = query_composer(hdfs_query, move_to_hfds_query(f'/home/bdm/data/{file}/{f}', f'/user/temporary/{file}/{date}'))
                    delete_query = query_composer(delete_query, f'rm /home/bdm/data/{file}/{f}')

            vm.exe(hdfs_query)
            print("All files moved successfully!")
            print("")
            print(f"Deleting /{file}...")
            vm.exe(delete_query)
            delete_dir(vm, f'/home/bdm/data/{file}')
            print("")
            print(f"All files from {file} were successfully processed")
            print("_"*100)
