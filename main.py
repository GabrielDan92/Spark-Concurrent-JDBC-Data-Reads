# execute an SQL query using Pandas
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
    jdbcDF = pd.read_sql(query, con=connection)
    
    
# execute an SQL query using PySpark and one executor core
# create the dataframe w/ a single partition
jdbcDF = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("user", os.environ['user']) \
    .option("password", os.environ['pass']) \
    .option("query", query) \
    .load()

    
# execute an SQL query using PySpark and all the available executor cores from the Spark Session
# create the dataframe w/ multiple partitions

# 1) pass the original query to dbtable as a subquery 
final_query = f'({query}) as q'

# 2) depends on your usecase, how many executor cores you have in the cluster
# what do you plan to do with the dataframe downstream, data size etc
partitionsCount = 100

# 3) I'm hardcoding the date values here, but I'll show below how to identify
# these values at run time if you don't know them upfront
min_date = '2020-01-01'
max_date = '2022-12-31'

jdbcDF = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("user", os.environ['user']) \
    .option("password", os.environ['pass']) \
    .option("dbtable", final_query) \
    .option("numPartitions", partitionsCount) \
    .option("partitionColumn", "date") \
    .option("lowerBound", f"{min_date}") \
    .option("upperBound", f"{max_date}") \
    .load()


# if you don't have a column that can be used for partitioning, create one yourself
original_query = "SELECT f1, f2, f3 FROM table"
final_query = f'(with cte as ({original_query}) select *, row_number() over (order by f1) as rn from cte) as q'


# if you don't know upfront the min and max values of your partition column, you can find that as well using SQL
query_min_max = f"SELECT min(date), max(date) from ({query}) q"
final_query = f"({query}) as q"

df_min_max = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("user", os.environ['user']) \
    .option("password", os.environ['pass']) \
    .option("query", query_min_max) \
    .load()

min = df_min_max.first()["min"]
max = df_min_max.first()["max"]

jdbcDF = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("user", os.environ['user']) \
    .option("password", os.environ['pass']) \
    .option("dbtable", final_query) \
    .option("numPartitions", partitionsCount) \
    .option("partitionColumn", "date") \
    .option("lowerBound", f"{min}") \
    .option("upperBound", f"{max}") \
    .load()
