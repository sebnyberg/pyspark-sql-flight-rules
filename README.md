# PySpark SQL Flight Rules

Learning how to use PySpark SQL is not as straightforward as one would hope.

This repository is an effort to summarize answers to scenarios I've found myself in as I've journeyed into PySpark land.

Note that this guide is not for PySpark RDD.

All snippets below assume that you have PySpark installed and available, and that the SparkSession is initialized like so:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('myapp').getOrCreate()
```

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [SparkContext and SparkSession](#sparkcontext-and-sparksession)
  - [Initialize a SparkSession](#initialize-a-sparksession)
  - [Retrieve the SparkContext from an existing SparkSession](#retrieve-the-sparkcontext-from-an-existing-sparksession)
  - [Retrieve the SQLContext](#retrieve-the-sqlcontext)
- [Reading files](#reading-files)
  - [I want to read a CSV file](#i-want-to-read-a-csv-file)
- [Simple operations](#simple-operations)
  - [Show (a preview of) DataFrame content](#show-a-preview-of-dataframe-content)
  - [Count (list) number of rows in the DataFrame](#count-list-number-of-rows-in-the-dataframe)
  - [Return all row values](#return-all-row-values)
  - [Return some (head) values](#return-some-head-values)
  - [Limit number of rows](#limit-number-of-rows)
  - [Select columns](#select-columns)
  - [Select and rename](#select-and-rename)
  - [Rename column in-place](#rename-column-in-place)
  - [Add (copy) a column](#add-copy-a-column)
  - [Filter rows](#filter-rows)
  - [Filter rows with SQL](#filter-rows-with-sql)
  - [Remove (drop) rows with null values](#remove-drop-rows-with-null-values)
  - [Drop column(s)](#drop-columns)
  - [Group by column and count occurrences](#group-by-column-and-count-occurrences)
  - [Print the column types (schema) of the DataFrame](#print-the-column-types-schema-of-the-dataframe)
  - [Statistical summary of columns in the DataFrame (avg, mean, std)](#statistical-summary-of-columns-in-the-dataframe-avg-mean-std)
  - [Convert from Pandas -> PySpark](#convert-from-pandas---pyspark)
  - [Convert from PySpark -> Pandas](#convert-from-pyspark---pandas)
- [Transformation examples](#transformation-examples)
  - [Change a column in-place using a function](#change-a-column-in-place-using-a-function)
  - [Add a new column with the same value on all rows](#add-a-new-column-with-the-same-value-on-all-rows)
  - [Concatenate two columns into a new column](#concatenate-two-columns-into-a-new-column)
  - [Add a dataframe to the bottom of another dataframe](#add-a-dataframe-to-the-bottom-of-another-dataframe)
  - [Add a dataframe to the right end of another dataframe](#add-a-dataframe-to-the-right-end-of-another-dataframe)
  - [Add a column from another dataframe](#add-a-column-from-another-dataframe)
  - [Rename many columns](#rename-many-columns)
- [User-Defined Functions (UDF)](#user-defined-functions-udf)
  - [UDF: Add or change a column using a lambda function](#udf-add-or-change-a-column-using-a-lambda-function)
  - [UDF: Add or change a column using an imported function](#udf-add-or-change-a-column-using-an-imported-function)
  - [UDF: calculate new column value based off several other columns in the same row](#udf-calculate-new-column-value-based-off-several-other-columns-in-the-same-row)
  - [UDF: custom function with more than one argument](#udf-custom-function-with-more-than-one-argument)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## SparkContext and SparkSession

### Initialize a SparkSession

```python
spark = pyspark.sql.SparkSession.builder.appName('myapp').getOrCreate()
```

### Retrieve the SparkContext from an existing SparkSession

The SparkContext is available as an attribute of your SparkSession:

```python
spark = pyspark.sql.SparkSession.builder.appName('myapp').getOrCreate()
sc = spark.sparkContext
```

### Retrieve the SQLContext

SQL Context is an old (1.x) name for SparkSession, see SparkSession

## Reading files

### I want to read a CSV file

If `header` is set to `False`, the header will be skipped.

```python
df = spark.read.csv('path/to/file.csv', sep=';', header=True, inferSchema=True)
```

## Simple operations

### Show (a preview of) DataFrame content

```python
df.show()
```

### Count (list) number of rows in the DataFrame

```python
df.count()
```

### Return all row values

```python
df.collect()
```

### Return some (head) values

```python
df.head(5)
```

### Limit number of rows

```python
df.limit(1)
```

### Select columns

```python
df.select('name', 'age')
```

### Select and rename

```python
df.select('name as first_name', 'age')
```

### Rename column in-place

```python
df.withColumnRenamed('name', 'first_name')
```

### Add (copy) a column

```python
df.withColumn('age_copy', df['age'])
```

### Filter rows

`where` and `filter` are analogous:

```python
old_people = df.where(df['age'] > 30)
quite_old_people = df.where(
  (df['age'] > 30)
  & (df['age'] < 40)
)
```

### Filter rows with SQL

```python
old_people = df.where('age > 30')
quite_old_people = df.where('age BETWEEN 30 AND 40')
```

### Remove (drop) rows with null values

`where` and `filter` are analogous:

```python
df.where(df['address'].isNotNull())
```

### Drop column(s)

```python
df.drop('name')
df.drop('name', 'age')
```

### Group by column and count occurrences

```python
df.groupBy('age').count()
```

### Print the column types (schema) of the DataFrame

```python
df.printSchema()
```

### Statistical summary of columns in the DataFrame (avg, mean, std)

```python
df.describe().show()
```

### Convert from Pandas -> PySpark

```python
pandas_df = pyspark_df.toPandas()
```

### Convert from PySpark -> Pandas

```python
pyspark_df = spark.createDataFrame(pandas_df)
```

## Transformation examples

### Change a column in-place using a function

DataFrame:

| name  | address     | city      |
| ----- | ----------- | --------- |
| Bob   | Brick St. 2 | Lund      |
| Alice | Olsvagen 12 | STOCKHOLM |

Goal:

| name  | address     | city      |
| ----- | ----------- | --------- |
| Bob   | Brick St. 2 | LUND      |
| Alice | Olsvagen 12 | STOCKHOLM |

```python
from pyspark.sql.functions import upper

df.withColumn('city', upper(df['city']))
```

### Add a new column with the same value on all rows

DataFrame:

| name  | address     |
| ----- | ----------- |
| Bob   | Brick St. 2 |
| Alice | Olsvagen 12 |

Goal:

| name  | address     | age |
| ----- | ----------- | --- |
| Bob   | Brick St. 2 | 28  |
| Alice | Olsvagen 12 | 28  |

Add a new column with a literal value using `lit`:

```python
from pyspark.sql.functions import lit

df.withColumn('age', lit(28))
```

### Concatenate two columns into a new column

DataFrame:

| first_name | last_name | address     | age |
| ---------- | --------- | ----------- | --- |
| Bob        | Barker    | Brick St. 2 | 28  |
| Alice      | Smith     | Olsvagen 12 | 28  |

Goal:

| first_name | last_name | address     | age | full_name   |
| ---------- | --------- | ----------- | --- | ----------- |
| Bob        | Barker    | Brick St. 2 | 28  | Bob Barker  |
| Alice      | Smith     | Olsvagen 12 | 28  | Alice Smith |

```python
from pyspark.sql.functions import concat, lit

df.withColumn('full_name', concat(df['first_name'], lit(' '), df['last_name']))
```

### Add a dataframe to the bottom of another dataframe

DataFrame 1:

| name  | address     |
| ----- | ----------- |
| Bob   | Brick St. 2 |
| Alice | Olsvagen 12 |

DataFrame 2:

| name  | address                |
| ----- | ---------------------- |
| Peter | Bellevue               |
| Tim   | Madison Circle Guarden |

Goal:

| name  | address                |
| ----- | ---------------------- |
| Bob   | Brick St. 2            |
| Alice | Olsvagen 12            |
| Peter | Bellevue               |
| Tim   | Madison Circle Guarden |

Use the sql function `union`:

```python
df1.union(df2).show()
```

### Add a dataframe to the right end of another dataframe

DataFrame 1:

| name  | address     |
| ----- | ----------- |
| Bob   | Brick St. 2 |
| Alice | Olsvagen 12 |

DataFrame 2:

| age | zip   |
| --- | ----- |
| 28  | 21533 |
| 28  | 21475 |

Goal:

| name  | address     | age | zip   |
| ----- | ----------- | --- | ----- |
| Bob   | Brick St. 2 | 28  | 21533 |
| Alice | Olsvagen 12 | 28  | 21475 |

Plan:

1. Add a temporary, join column in both dataframes which is identical
2. Join both data frames with a full outer join
3. Drop the temporary join column

```python
from pyspark.sql.functions import monotonically_increasing_id

joined_df = (
    df1
        .withColumn('joincol', monotonically_increasing_id())
        .join(
            df2
                .withColumn('joincol', monotonically_increasing_id()),
                on='joincol',
                how='outer'
        )
        .drop('joincol')
)
joined_df.show()
```

### Add a column from another dataframe

DataFrame 1:

| name  | address     |
| ----- | ----------- |
| Bob   | Brick St. 2 |
| Alice | Olsvagen 12 |

DataFrame 2:

| age | zip   |
| --- | ----- |
| 28  | 21533 |
| 28  | 21475 |

Goal:

| name  | address     | age |
| ----- | ----------- | --- |
| Bob   | Brick St. 2 | 28  |
| Alice | Olsvagen 12 | 28  |

Plan:

1. Select the age column from the second dataframe
2. Add a temporary, join column in both dataframes which is identical
3. Join both data frames with a full outer join
4. Drop the temporary join column

```python
from pyspark.sql.functions import monotonically_increasing_id

joined_df = (
    df1
        .withColumn('joincol', monotonically_increasing_id())
        .join(
            df2
                .select('age')
                .withColumn('joincol', monotonically_increasing_id()),
                on='joincol',
                how='outer'
        )
        .drop('joincol')
)
joined_df.show()
```

### Rename many columns

DataFrame:

| name  | address     | age |
| ----- | ----------- | --- |
| Bob   | Brick St. 2 | 28  |
| Alice | Olsvagen 12 | 28  |

Goal:

| first_name | person_address | person_age |
| ---------- | -------------- | ---------- |
| Bob        | Brick St. 2    | 28         |
| Alice      | Olsvagen 12    | 28         |

Plan:

1. Create a list of select expressions: ["name as first_name", 'address as person_address', 'age as person_age']
2. Use `selectExpr` to rename columns

```python
renames = {
    'name': 'first_name',
    'address': 'person_address',
    'age': 'person_age'
}
rename_expr = [
    '{} as {}'.format(key, value)
    for key, value
    in renames.items()
]

df.selectExpr(rename_expr).show()
```

## User-Defined Functions (UDF)

### UDF: Add or change a column using a lambda function

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

wrap_in_quotes = udf(lambda text: '"' + text + '"', StringType())

member_pages.withColumn('quoted_biography', wrap_in_quotes(member_pages['biography']))
```

### UDF: Add or change a column using an imported function

**IMPORTANT**: importing external functions will not work unless you first add the Python file to the SparkContext. Failing to do so will result in an ambiguous error.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark.sparkContext.addPyFile('src/util/string_helpers.py)
from string_helpers import wrap_in_quotes as _wrap_in_quotes

wrap_in_quotes = udf(_wrap_in_quotes, StringType())

member_pages.withColumn('quoted_biography', wrap_in_quotes(member_pages['biography']))
```

### UDF: calculate new column value based off several other columns in the same row

Note: there are better ways to do what this example does, for example with the function `concat` or `concat_ws`. This is simply for demonstrational purposes.

The function `struct` can be used to create a column which contains many other columns.

DataFrame:

| first_name | last_name | address     | age |
| ---------- | --------- | ----------- | --- |
| Bob        | Barker    | Brick St. 2 | 28  |
| Alice      | Smith     | Olsvagen 12 | 28  |

```python
from pyspark.sql.functions import struct

df.withColumn('name_arr', struct('first_name', 'last_name')).show()
```

Result:

| first_name | last_name | address     | age | full_name_arr  |
| ---------- | --------- | ----------- | --- | -------------- |
| Bob        | Barker    | Brick St. 2 | 28  | [Bob, Barker]  |
| Alice      | Smith     | Olsvagen 12 | 28  | [Alice, Smith] |

Using this method, we can use several columns inside our user-defined function:

```python
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StringType

join_columns_with_space = udf(lambda row: ' '.join(row), StringType())
df.withColumn('full_name', join_columns_with_space(struct('first_name', 'last_name'))).show()

# this works too (the row is a named tuple):
concat_names = udf(lambda row: row['first_name'] + ' ' + row['last_name'], StringType())
df.withColumn('full_name', concat_names(struct('first_name', 'last_name'))).show()
```

Result:

| first_name | last_name | address     | age | full_name   |
| ---------- | --------- | ----------- | --- | ----------- |
| Bob        | Barker    | Brick St. 2 | 28  | Bob Barker  |
| Alice      | Smith     | Olsvagen 12 | 28  | Alice Smith |

### UDF: custom function with more than one argument

This example works similarly to the built-in function `concat_ws`. We want to pass the separator and some columns to our own `join_with_separator` function.

DataFrame:

| first_name | last_name | address     | age |
| ---------- | --------- | ----------- | --- |
| Bob        | Barker    | Brick St. 2 | 28  |
| Alice      | Smith     | Olsvagen 12 | 28  |

```python
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StringType

def join_with_separator(sep, *args):
  def _join_with_separator(row):
    return sep.join(row)

  return udf(_join_with_separator, StringType())(struct(*args))

grs_locations.withColumn('full_name', join_with_separator(' ', df['first_name'], df['last_name'])).show()
```

Result:

| first_name | last_name | address     | age | full_name   |
| ---------- | --------- | ----------- | --- | ----------- |
| Bob        | Barker    | Brick St. 2 | 28  | Bob Barker  |
| Alice      | Smith     | Olsvagen 12 | 28  | Alice Smith |
