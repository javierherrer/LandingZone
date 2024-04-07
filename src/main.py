from api_zip_hdfs import download_unemployment
from move_files_from_resources import zip_and_move_folder
from data_collector import execute_data_collector
from persistent_collector import persistent_collector
from clean_temporal_zone import clean_temporal_zone

from cli_parser import get_parser


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