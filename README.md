# ParquetDataExporter

A working data exporter that can extract data from Oracle database and load data into parquet files locally. The Parquet files generated are mapped to each sql query in the order of sqls that are provided in export.sql(contains a dummy query on employees table, you can replace with your own queries) or a separate sqlfile of your choise during the runtime. 

## Pre-Req:

A Python environment with version >=3.9 is required to install `oracledb` module.

Required modules can be installed from `requirements.txt` using following command:

```
pip install -r requirements.txt
```

## Working method

### SQL file

The SQL file should only contain SQL queries, this doesn't support executing anonymous PL/SQL code, you may execute existing packages delimited by `;` Ex: exec dbms_lock.sleep(10);

### oracledb

This exporter utilizes `oracledb` module configured in thick mode to support additional features like Kerberos authentication, NNE etc. Complete details are provided in: [featuresummary](https://python-oracledb.readthedocs.io/en/latest/user_guide/appendix_a.html#featuresummary)

To utilize the thick mode, atleast Oracle's Instant client is required to be installed on the machine where you run the exporter.

Instant client is available at https://www.oracle.com/in/database/technologies/instant-client/downloads.html for download as per your Operating System.

### Usage

```
Usage: python ora2parquetdump.py <username> <password> <host> <port> <service_name> <sql_file> <loglevel>
log level can be one of following: 
 DEBUG 
 WARNING 
 CRITICAL 
 ERROR 
 INFO
```
