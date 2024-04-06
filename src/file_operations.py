import datetime

def transfer_source_files(vm, localfilepath, remotefilepath):
    vm.transfer_files(localfilepath, remotefilepath)

def delete_file_in_dir(vm, dir):
    cmd = (f'rm {dir}')
    out = vm.exe(cmd)
    print(out)

def unzip_source_files(vm):
    cmd = ('cd /home/bdm/data \n'
           'unzip data')
    out = vm.exe(cmd)
    print(out)

def get_files_in_folder(vm, dir):
    cmd = (f'ls {dir}')
    return vm.exe(cmd)

def move_file_to_hdfs(vm, filepath, hdfspath):
    cmd = (f'/home/bdm/BDM_Software/hadoop/bin/hdfs dfs -put {filepath} {hdfspath}')
    print(f"cmd: {cmd}")
    vm.exe(cmd)

def check_if_hdfs_dir_exists(vm, path):
    cmd = (f'/home/bdm/BDM_Software/hadoop/bin/hdfs dfs -test -d /user/{path} \n'
           f'echo $?')
    o = vm.exe(cmd)[0]
    if o == "0":
        return True
    elif o == "1":
        return False

def make_hdfs_dir(vm, dir):
    cmd = (f'/home/bdm/BDM_Software/hadoop/bin/hdfs dfs -mkdir /user/{dir}')
    vm.exe(cmd)

def delete_dir(vm, dir):
    cmd = (f'rmdir {dir}')
    vm.exe(cmd)

def query_composer(q: str, add: str):
    return q + add + "\n"

def move_to_hfds_query(filepath, hdfspath):
    return f'/home/bdm/BDM_Software/hadoop/bin/hdfs dfs -put {filepath} {hdfspath}'
