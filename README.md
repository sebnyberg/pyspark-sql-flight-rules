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

- [Repositories](#repositories)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Reading files

### Read a CSV file

If `header` is set to `False`, the header will be skipped.

```python
df = spark.read.csv('path/to/file.csv', sep=';', header=True, inferSchema=True)
```

## Merging dataframes

### Adding a dataframe to the bottom of another dataframe

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

### Merging one dataframe to the end of another dataframe

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

## Change column values

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
