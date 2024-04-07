# Landing Zone

P1 Project of BDM

## Description

This project is designed to demonstrate the flexibility and power of Python 3.11 in handling data processing tasks. By leveraging the Python ecosystem, the project aims to provide a robust framework for working with various datasets, including the ability to dynamically fetch unemployment data via an API. This project is ideal for data scientists, researchers, and anyone interested in data analysis and manipulation.

## Getting Started

### Prerequisites

Before you begin, ensure you have the following installed:
- Python 3.11 - You can download it from [python.org](https://www.python.org/downloads/).

### Installation

1. Clone the repository to your local machine:
   ```
   git clone https://github.com/your-repository-url.git
   ```
2. Navigate to the project directory:
   ```
   cd your-project-directory
   ```
3. Install the required dependencies using the `requirements.txt` file:
   ```
   pip install -r requirements.txt
   ```

### Setting Up Your Environment

Ensure all the data you wish to process is placed in the `resources` folder. Note that unemployment data will be fetched dynamically via an API, so there's no need to manually download this dataset.

## Usage

To run the project, use the following command from the root directory of the repository:

```
python main.py
```

### Customizing Parameters

The `main.py` script is designed to be flexible. You can modify the following parameters to suit your needs:

- **HOST_IP**: Adjust the VM IP address
- **HDFS_PORT**: Adjust the hdfs port
- **MONGO_PORT**: Adjust the mongoDB port
- **VM_USER**: Adjust the user inside the Virtual Machine
- **VM_PASS**: Add the Virtual Machine password
- **NAME_MONGO_DB**: Name of the mongoDB  database. 

