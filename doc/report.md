# Project (P1): Landing Zone

_Big Data Management, Master in Data Science, Barcelona School of Informatics_

Javier Herrer Torres (javier.herrer.torres@estudiantat.upc.edu)
Ignacio Lloret Lorente (ignacio.lloret@estudiantat.upc.edu)
Arnau Torreruella Ramos (arnau.torreruella@estudiantat.upc.edu)

---

[GitHub repository](https://github.com/javierherrer/LandingZone/)

## Introduction



## Barcelona Rentals

### Data Persistence Loader

A document store like MongoDB is a good choice for implementing a data persistence loader for the Idealista dataset for several reasons:

1. Schema Flexibility: MongoDB's schema-less nature allows it to store JSON documents directly, which is ideal since the data from Idealista's Search API is already in JSON format. This means there's no need for complex data transformations or mappings.
2. Handling of Semi-structured Data: The data obtained from the API is semi-structured and can vary in terms of the fields present in each document. MongoDB can easily handle this variability.
3. Geospatial Support: MongoDB has built-in geospatial features that can be leveraged for queries based on location, which is beneficial for a dataset that includes geographic information like property listings.
4. Scalability: MongoDB can handle large volumes of data and provides horizontal scalability through sharding, which can be useful for managing the high volume of listings and potential duplicates over time.
5. Efficient Querying: MongoDB offers powerful indexing options, including text and geospatial indexes, which can make searching through listings based on various attributes (like location, price, etc.) very efficient.
6. Aggregation Framework: MongoDB's aggregation framework can be used to process and aggregate data, such as grouping listings by date or location, which can be useful for analyzing the dataset.
7. Duplication Handling: Given that many listings may be duplicated across different days, MongoDB's flexible data model allows for easy implementation of deduplication logic.
8. Date Handling: Each JSON document is tagged with a date in the filename, and MongoDB has robust support for date fields, making it straightforward to query documents based on timeframes.
