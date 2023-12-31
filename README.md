# My Hive UDFs

## Description

This project is for creating a repository of Hive User Defined Functions (UDFs) which can be included as modules in
other projects.

## Environment

This project is developed and tested under the following environment:

- Java Development Kit (JDK) 8
- Apache Hive 3.1.3

## Contents

The repository includes the following UDFs:

- ifnull
- countif
- strpos

## Usage

1. Build UDF from gradle
2. Upload UDF jar file on your HDFS
3. Execute the query below.

```hiveql
ADD JAR hdfs:/your-udf-path/your-udf.jar;
CREATE TEMPORARY FUNCTION your_udf AS 'your.package.YourUDFClassName';
```
