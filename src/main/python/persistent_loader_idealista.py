from pymongo import MongoClient
import json
import os

# Connect to MongoDB using a docker container
client = MongoClient('localhost', 27017) 

# Access database
db = client['idealista']

# Access collection
collection = db['listings']

# Path to directory containing JSON files
directory = '../../../resources/idealista'

# Iterate through JSON files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        with open(os.path.join(directory, filename), 'r') as file:
            # Load JSON data
            data = json.load(file)
            # Insert data into MongoDB collection
            try: collection.insert_many(data)
            except TypeError as e: print(e)


result = collection.find({})
next(result)


