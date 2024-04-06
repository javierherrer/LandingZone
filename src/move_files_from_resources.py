import zipfile
import shutil
import os

def zip_and_move_folder(source_folder, destination_folder, zip_filename):
    # Create a zip file
    zip_path = os.path.join(destination_folder, zip_filename)
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        # Walk through the source folder and add all files and subdirectories to the zip file
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, source_folder)
                zipf.write(file_path, rel_path)

    # Move the zip file to the destination folder, overwriting if it already exists
    try: os.replace(zip_path, os.path.join(destination_folder, zip_filename))
    except FileNotFoundError: pass  # File does not exist yet, no need to overwrite


if __name__ == '__main__':
    # Example usage:
    source_folder = '../resources'
    destination_folder = 'data'
    zip_filename = 'data.zip'

    zip_and_move_folder(source_folder, destination_folder, zip_filename)
