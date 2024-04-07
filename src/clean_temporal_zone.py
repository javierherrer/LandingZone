from file_operations import remove_hdfs_dir
from vm_connection import VM


def clean_temporal_zone(args):
    vm = VM(hostname=args.host_ip, port=22, username=args.vm_user, password=args.vm_pass)
    
    remove_hdfs_dir(vm=vm, dir=vm._DIR_TEMPORAL)
