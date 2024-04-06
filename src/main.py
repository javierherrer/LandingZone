from api_zip_hdfs import download_unemployment
from move_files_from_resources import zip_and_move_folder
from data_collector import execute_data_collector
from persistent_collector import persistent_collector


def pipeline():
    download_success = download_unemployment()
    if download_success: zip_and_move_success = zip_and_move_folder('../resources', 'data', 'data.zip')
    if zip_and_move_success: execute_data_collector()
    persistent_collector()

if __name__ == '__main__':
    pipeline()