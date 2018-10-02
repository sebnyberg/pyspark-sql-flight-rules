# PySpark SQL Flight Rules

Learning how to use PySpark SQL is not all so straightforward as one would hope. This repository serves as a collection of answers to common scenarios you might find yourself in as you embark on your journey into pyspark sql.

Note that this guide is not for PySpark RDD.

All snippets below assume that you have pyspark installed and available, and that the spark session is initialized like so:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('myapp').getOrCreate()
```

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Reading files](#reading-files)
  - [Read a CSV file](#read-a-csv-file)
- [Merging dataframes](#merging-dataframes)
  - [Adding a dataframe to the bottom of another dataframe](#adding-a-dataframe-to-the-bottom-of-another-dataframe)
  - [Merging one dataframe to the end of another dataframe](#merging-one-dataframe-to-the-end-of-another-dataframe)
- [Adding columns](#adding-columns)
  - [Add a new column with the same value for all rows](#add-a-new-column-with-the-same-value-for-all-rows)
  - [Add a column from another dataframe](#add-a-column-from-another-dataframe)
- [Change column values](#change-column-values)
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

## DataFrame operations

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

### DataFrame -> DataFrame

#### I want to limit the number of rows

```python
df.limit(1).show()
```

#### I want to select some columns

```python
df.select('name', 'age').show()
df.select(['name', 'age']).show()
```

### I want to convert my PySpark DataFrame to a Pandas DataFrame

```python
df.toPandas()
```

### I want to convert my Pandas DataFrame to a PySpark DataFrame

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
