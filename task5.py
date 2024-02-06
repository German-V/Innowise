# %%
from configparser import ConfigParser
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, dense_rank
from pyspark.sql.functions import desc
from pyspark.sql.functions import col, when
from pyspark.sql.functions import max, avg, min, count, sum, expr


spark = SparkSession.builder.master('local[*]').appName("task5").config('spark.jars', 'postgresql-42.7.1.jar').getOrCreate()


def get_table(properties: dict, url: str, table_name: str, spark: SparkSession):
    return spark.read.jdbc(
        url=url,
        properties=properties,
        table=table_name
    )

parser = ConfigParser()
parser.read('database.ini')

db = {}
if parser.has_section('postgresql'):
    params = parser.items('postgresql')
    for param in params:
        db[param[0]] = param[1]
else:
    raise Exception('Section {0} not found in the {1} file'.format(section, filename))

properties = db
connection_str = "jdbc:postgresql://localhost:5432/pagila"
df_film_list = get_table(properties, connection_str, 'film_list', spark)
df_film = get_table(properties, connection_str, 'film', spark)
df_actor = get_table(properties, connection_str, 'actor', spark)
df_city = get_table(properties, connection_str, 'city', spark)
df_inventory = get_table(properties, connection_str, 'inventory', spark)
df_customer = get_table(properties, connection_str, 'customer', spark)
df_rental = get_table(properties, connection_str, 'rental', spark)
df_address = get_table(properties, connection_str, 'address', spark)
df_category = get_table(properties, connection_str, 'category', spark)
df_film_category = get_table(properties, connection_str, 'film_category', spark)


# %%
df_film_actor = get_table(properties, connection_str, 'film_actor', spark)

# %%
df_film_list.groupBy("category").agg(count("*").alias("film_count")).show()


# %%
joined_df = df_film.alias("f").join(df_film_actor.alias("fa"), col("f.film_id") == col("fa.film_id"), 'inner')


df_actor.alias("a")\
.select("a.actor_id")\
.join(joined_df, col("a.actor_id") == col("fa.actor_id"), 'inner')\
.groupBy("a.actor_id")\
.agg(sum(col("f.rental_duration")).alias('rent_sum'))\
.orderBy(col("rent_sum").desc())\
.limit(10).show()



# %%

df_film_list\
.select(col('category'), 
        sum(col("price")).over(Window().partitionBy("category")).alias("sum_price"))\
.orderBy(col('sum_price').desc()).distinct()\
.withColumn("num", dense_rank().over(Window.orderBy(desc("sum_price")))).filter("num=1").show()


# %%
df_film.alias("f").join(df_inventory.alias("i"), col("f.film_id")==col("i.film_id"), 'anti')\
    .select("f.title").distinct().show()

# %%
df_temp = df_actor.alias("a")\
    .join(df_film_actor.alias("fa"), col("a.actor_id") == col("fa.actor_id"))\
    .join(df_film_category.alias("fc"), col("fa.film_id") == col("fc.film_id"))\
    .join(df_category.alias("c"), col("c.category_id") == col("fc.category_id"))\
    .select("a.first_name", "a.last_name","a.actor_id")\
    .where(col("c.name")=='Children')\
    .groupBy("a.first_name","a.last_name").agg(count("a.actor_id").alias('total'))\
    .withColumn("dn", dense_rank().over(Window.orderBy(col('total').desc())).alias('dn'))
df_temp.select("first_name", "last_name", "total")\
    .where(col("dn")<=3).show()

# %%
df_city.alias("c")\
    .join(df_address.alias("a"), col("a.city_id")==col("c.city_id"), 'left')\
    .join(df_customer.alias("c2"), col("a.address_id")==col("c2.address_id"), 'inner')\
    .select("city", "c2.customer_id", "c2.active")\
    .groupBy("city")\
    .agg(count("c2.customer_id").alias("customer_count"),
        sum("c2.active").alias("active"),
        count(expr("CASE WHEN c2.active = 0 THEN 1 END")).alias("inactive"))\
    .orderBy(col("inactive").desc()).show()

# %%
df_category.alias("c")\
.join(df_film_category.alias("fc"), col("fc.category_id") == col("c.category_id"))\
.join(df_inventory.alias("i"), col("i.film_id") == col("fc.film_id"))\
.join(df_rental.alias("r"), col("r.inventory_id") == col("i.inventory_id"))\
.join(df_customer.alias("c2"), col("c2.customer_id") == col("r.customer_id"))\
.join(df_address.alias("a"), col("a.address_id") == col("c2.address_id"))\
.join(df_city.alias("c3"), (col("c3.city_id") == col("a.city_id")) & (col("c3.city").like('a%')))\
.groupBy("c.name")\
.agg(sum(col("r.return_date") - col("r.rental_date")).alias("rent"))\
.orderBy(col("rent").desc())\
.limit(1)\
.union(
    df_category.alias("c")
    .join(df_film_category.alias("fc"), col("fc.category_id") == col("c.category_id"))
    .join(df_inventory.alias("i"), col("i.film_id") == col("fc.film_id"))
    .join(df_rental.alias("r"), col("r.inventory_id") == col("i.inventory_id"))
    .join(df_customer.alias("c2"), col("c2.customer_id") == col("r.customer_id"))
    .join(df_address.alias("a"), col("a.address_id") == col("c2.address_id"))
    .join(df_city.alias("c3"), (col("c3.city_id") == col("a.city_id")) & (col("c3.city").like('%-%')))
    .groupBy("c.name")
    .agg(sum(col("r.return_date") - col("r.rental_date")).alias("rent"))
    .orderBy(col("rent").desc())
    .limit(1)
    )\
.show()


