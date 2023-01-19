## Objective:
The project's objective is to refactor a function to use PySpark's JDBC functionality to execute an SQL query in a relational database instead of executing the query through Panda's `read_sql()`. By following a similar approach in a production Amazon EMR environment, I've managed to improve the performance of my queries and decrease the execution time with **more than 95%**.

## How is this even possible?
This is a fair question. You don't read everyday about 95% improvements in a project by just changing one library with another, but there is a reason why 80% of the Fortune 500 companies use Apache Spark (*source: https://spark.apache.org/*). 
<br />
<br />
Apache Spark is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters. It is a powerful tool for big data processing, but it can also be used to access data stored in databases. In this repo, I will show you how to use Spark’s JDBC read option to access data from a database in a distributed fashion, as well as why Pandas falls short when trying to execute an SQL query that returns millions of records.

## Pandas example:
The syntax for executing SQL queries in Pandas is pretty straightforward. You need a `Connection` object that contains your host, port, user, password, database name and depending on your environment, `ssl` set to True. Some other options can be passed as well to this object but for the purpose of this example, I won't explore them. 
<br />
<br />
Executing a generic SQL query in a Vertica database using Pandas and passing the mandatory arguments stored as environment variables looks like this:
```
query = 'SELECT f1, f2, f3 FROM table'
conn_info = {
    'host': os.environ["host"],
    'port': os.environ["port"],
    'user': os.environ["user"],
    'password': os.environ["pass"],
    'database': os.environ["db_name"],
    'ssl': True,
}

with vertica_python.connect(**conn_info) as connection:
    df = pd.read_sql(query, con=connection)
```

## PySpark example:
The PySpark syntax is a little bit more complex, but the performance gains are so high that it's difficult making a convincing case for not using it. Let's see how it looks, following the same example as above (Vertica database, environment variables):
```
query = 'SELECT f1, f2, f3 FROM table'
partitionsCount = 50

df = spark.read \
    .format("jdbc") \
    .option("driver", "com.vertica.jdbc.Driver") \
    .option("url", f"jdbc:vertica://{os.environ["host"]}:{os.environ["port"]}") \
    .option("user", os.environ['user']) \
    .option("password", os.environ['pass']) \
    .option("ssl", "True") \
    .option("sslmode", "require") \
    .option("dbtable", query) \
    .option("numPartitions", partitionsCount) \
    .option("partitionColumn", "date") \
    .option("lowerBound", f"{min_date}") \
    .option("upperBound", f"{max_date}") \
    .load()
```

## So why is Pandas not the best choice to execute a query returning millions of rows?
It's pretty simple: Pandas has been designed to be highly optimized for single-threaded data processing. Pandas creator (Wes McKinney) said that he was not thinking about analyzing 100GB or 1TB datasets when he built Pandas. Trying to use Pandas with large datasets (or trying to load millions of rows through ```read_sql()``` is not and will never be a great experience.

<img width="300" alt="image" src="https://user-images.githubusercontent.com/36746674/213457508-8f9fa87e-1351-468c-8dbe-1465b2d1293b.png">

## What about Spark?
Configuring it **properly**, Spark will execute the SQL query concurrently in JVM. When transferring large amounts of data between Spark and an external RDBMS by default JDBC data sources loads data sequentially using a single executor thread, which can significantly slow down your application performance, and potentially exhaust the resources of your source system. In order to read data concurrently, the Spark JDBC data source must be configured with appropriate partitioning information so that it can issue multiple concurrent queries to the external database. *source: https://luminousmen.com/post/spark-tips-optimizing-jdbc-data-source-reads*

<img width="500" alt="image" src="https://user-images.githubusercontent.com/36746674/213458672-4143613f-f44d-48a0-94ee-022c8fc776ef.png">

## What are those passed arguments?
I won't go into details about the first 8 options since they are self explanatory (if not, Spark's official documentation might be a good read: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html). But I'm going to explain the last four, since these options are the ones that make the magic happen :slightly_smiling_face:
- `numPartitions`: The maximum number of partitions that can be used for parallelism in table reading. This also determines the maximum number of concurrent JDBC connections. The correct number of partitions depends on the memory assigned to your executor instances (JVMs instances), how large is your dataset, and how many executor cores (number of slots available to run tasks in parallel) you have set up in your Spark Session. Keep in mind that Spark assigns one task per partition, so each partition will be processed by one executor core.
- These options describe how to partition the table when reading in parallel from multiple workers:
    - `partitionColumn`: partitionColumn must be a numeric, date, or timestamp column from the table in question. For the best possible performance, the column values must be as evenly distributed as possible and have high cardinality. Avoid data skewness. 
    - `lowerBound`, `upperBound`: Notice that lowerBound and upperBound are just used to decide the partition stride, not for filtering the rows in table. All rows in the table will be partitioned and returned. 
- What if we don’t know upfront the lower and upper bounds of our column? 
    - This is a good example where we can use Pandas to identify the `lowerBound` and `upperBound` through Pandas. Once we know that, we can pass the results as arguments to the Spark lower/upper bound parameters.
    ```
    min_max_query = f"SELECT min(date), max(date) from ({query}) as q"
    df_min_max = pd.read_sql(min_max_query, con=connection)
    min_date = df_min_max['min'].dt.date.values[0]
    max_date = df_min_max['max'].dt.date.values[0]
    ```
- What if we don’t have any numeric, date, or timestamp column, or it has skewed data?
    - Not a problem. We know SQL, so using a CTE and one Window Function to generate a numeric column through the `row_number()` function should be a piece of cake:
    ```
    with cte as
    (
        SELECT f1, f2, f3
        FROM table
    )
    select *, row_number() over (order by f1) as rn from cte
    ```

 

