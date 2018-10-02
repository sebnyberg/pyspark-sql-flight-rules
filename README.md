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

- [Reading files](#reading-files)
  - [I want to read a CSV file](#i-want-to-read-a-csv-file)
- [DataFrame operations](#dataframe-operations)
  - [DataFrame properties](#dataframe-properties)
    - [I want to know the types of the DataFrame columns](#i-want-to-know-the-types-of-the-dataframe-columns)
    - [I want to know the number of rows in the DataFrame](#i-want-to-know-the-number-of-rows-in-the-dataframe)
    - [I want a summary of the DataFrame](#i-want-a-summary-of-the-dataframe)
  - [DataFrame -> DataFrame](#dataframe---dataframe)
    - [I want to limit the number of rows](#i-want-to-limit-the-number-of-rows)
    - [I want to select columns by name](#i-want-to-select-columns-by-name)
    - [I want to convert my PySpark DataFrame to a Pandas DataFrame](#i-want-to-convert-my-pyspark-dataframe-to-a-pandas-dataframe)
    - [I want to convert my Pandas DataFrame to a PySpark DataFrame](#i-want-to-convert-my-pandas-dataframe-to-a-pyspark-dataframe)
  - [DataFrame -> Rows](#dataframe---rows)
    - [I want to extract top rows](#i-want-to-extract-top-rows)
    - [I want to extract data from the DataFrame as a list](#i-want-to-extract-data-from-the-dataframe-as-a-list)
- [Merging dataframes](#merging-dataframes)
  - [Add a dataframe to the bottom of another dataframe](#add-a-dataframe-to-the-bottom-of-another-dataframe)
  - [Add a dataframe to the right end of another dataframe](#add-a-dataframe-to-the-right-end-of-another-dataframe)
- [Adding columns](#adding-columns)
  - [Add a new column with the same value for all rows](#add-a-new-column-with-the-same-value-for-all-rows)
  - [Add a column from another dataframe](#add-a-column-from-another-dataframe)
- [Changing column values](#changing-column-values)
  - [Change column type](#change-column-type)
- [Renaming columns](#renaming-columns)
  - [Rename a single column, keep others the same](#rename-a-single-column-keep-others-the-same)
  - [Rename many columns](#rename-many-columns)
- [Conditional filtering](#conditional-filtering)
  - [Show rows which are (not) null](#show-rows-which-are-not-null)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Reading files

### I want to read a CSV file

If `header` is set to `False`, the header will be skipped.

```python
df = spark.read.csv('path/to/file.csv', sep=';', header=True, inferSchema=True)
```

## Basic DataFrame operations

### DataFrame properties

#### I want to know the types of the DataFrame columns

```python
df.printSchema()
```

#### I want to know the number of rows in the DataFrame

```python
df.count()
```

#### I want a summary of the DataFrame

```python
df.show()
```

#### I want a statistical summary of columns in the DataFrame (avg, mean, std)

```python
df.describe().show()
```

### DataFrame -> DataFrame

#### I want to select N rows

```python
df.limit(1).show()
```

#### I want to select columns by name

```python
df.select('name', 'age').show()
df.select(['name', 'age']).show()
```

#### I want to filter rows based off a column value

The filter function `filter` is an alias for `where`. Both can be used interchangably.

Beware that in SQL, the equality operator is a single equals sign `=`. Below are some examples with different ways of referencing dataframe columns, I recommend using the `df['column_name']` or `df.column_name` syntax because it gives you autocompletion.

```python
df.filter(df.name == 'Bob').show()
df.filter(df['name'] == 'Bob').show()
df.filter(col('name') == 'Bob').show()
df.filter("name = 'Bob'").show()
```

#### I want to filter rows based off many column values

Filtering many column values requires you to use bitwise operators (and: `&`, or: `|`) and to evaluate conditions inside parenthesis.

```python
df.where((df.address.contains('st')) & (df.age > 25)).show()
df.where("address LIKE '%st%' AND age > 25").show()
```

#### I want to convert my PySpark DataFrame to a Pandas DataFrame

```python
pandas_df = df.toPandas()
```

#### I want to convert my Pandas DataFrame to a PySpark DataFrame

```python
data = {
    'name': ['Alice', 'Bob'],
    'age': [28, 28]
}
pandas_df = pd.DataFrame(data=data)
pyspark_df = spark.createDataFrame(pandas_df)
```

### DataFrame -> Rows

#### I want to extract top rows

```python
df.head(5)
```

#### I want to extract data from the DataFrame as a list

```python
df.collect()
```

## Merging dataframes

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

## Adding columns

### Add a new column with the same value for all rows

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

## Changing column values

### Change column type

DataFrame:

| name    | address       | zip   |
| ------- | ------------- | ----- |
| 'Bob'   | 'Brick St. 2' | 21522 |
| 'Alice' | 'Olsvagen 12' | 31475 |

Goal:

| name    | address       | zip     |
| ------- | ------------- | ------- |
| 'Bob'   | 'Brick St. 2' | '21522' |
| 'Alice' | 'Olsvagen 12' | '31475' |

## Renaming columns

### Rename a single column, keep others the same

DataFrame:

| name  | address     | age |
| ----- | ----------- | --- |
| Bob   | Brick St. 2 | 28  |
| Alice | Olsvagen 12 | 28  |

Goal:

| first_name | address     | age |
| ---------- | ----------- | --- |
| Bob        | Brick St. 2 | 28  |
| Alice      | Olsvagen 12 | 28  |

```python
df.selectExpr(['name as first_name', *df.drop('name').columns]).show()
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
