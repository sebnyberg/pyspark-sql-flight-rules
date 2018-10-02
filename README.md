# PySpark SQL Flight Guide

Learning how to use PySpark SQL is not all so straightforward as one would hope. This repository serves as a collection of answers to common scenarios you might find yourself in as you embark on your journey into pyspark sql.

Note that this guide is not for PySpark RDD.

All snippets below assume that you have pyspark installed and available, and that the spark session is initialized like so:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('myapp').getOrCreate()
```

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Repositories](#repositories)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Reading files

### Read a CSV file

If `header` is set to `False`, the header will be skipped.

```python
df = spark.read.csv('path/to/file.csv', sep=';', header=True, inferSchema=True)
```

## Conditional filtering

### Show rows which are (not) null

Given the dataframe

| name  | address     |
| ----- | ----------- |
| Bob   | Brick St. 2 |
| Alice | null        |

You can show rows which are (not) null with:

```python
df.where(df['address'].isNull()).show()
df.where(df['address'].isNotNull()).show()
```

Result:

| name  | address |
| ----- | ------- |
| Alice | null    |

and

| name | address     |
| ---- | ----------- |
| Bob  | Brick St. 2 |
