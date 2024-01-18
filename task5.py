# %%
from configparser import ConfigParser
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, rank, row_number
from pyspark.sql.functions import desc
from pyspark.sql.functions import col

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
df_film_list.createOrReplaceTempView("film_list")
df_film.createOrReplaceTempView("film")
df_actor.createOrReplaceTempView("actor")
df_film_actor.createOrReplaceTempView("film_actor")
df_inventory.createOrReplaceTempView("inventory")
df_category.createOrReplaceTempView("category")
df_film_category.createOrReplaceTempView("film_category")
df_city.createOrReplaceTempView("city")
df_customer.createOrReplaceTempView("customer")
df_rental.createOrReplaceTempView("rental")
df_address.createOrReplaceTempView("address")

# %%
spark.sql("select category, count(*) as film_count from film_list fl \
group by category \
order by film_count desc \
").collect()


# %%
spark.sql("select actor_id, (\
    select sum(rental_duration) from film f\
    join film_actor fa on f.film_id=fa.film_id\
    where fa.actor_id = a.actor_id\
    group by fa.actor_id\
    ) as rent_sum\
    from actor a\
    order by rent_sum desc \
    limit 10").collect()


# %%

spark.sql("select distinct category, sum(price) over(partition by category) sum_price from film_list fl \
order by sum_price desc").withColumn("num", dense_rank().over(Window.orderBy(desc("sum_price")))).filter("num=1").show()


# %%
spark.sql("select distinct title from film f \
where not exists (select film_id from inventory i where f.film_id = i.film_id) ").show()

# %%
spark.sql("select first_name, last_name, total from( \
select dense_rank() over(order by count(a.actor_id) desc) dn, a.first_name, a.last_name, count(a.actor_id) as total  from actor a \
join film_actor fa on a.actor_id = fa.actor_id \
join film_category fc on fa.film_id = fc.film_id \
join category c on c.category_id =fc.category_id \
where c.name = 'Children' \
group by a.first_name,a.last_name ) b \
where dn <= 3").show()

# %%
spark.sql("select city, count(c2.customer_id), sum(c2.active) active, count(*) filter(where c2.active=0) inactive from city c \
left join address a on a.city_id = c.city_id \
join customer c2 ON a.address_id = c2.address_id \
group by city \
order by inactive desc").show()

# %%
spark.sql("select name, rent from(( \
select name, sum(r.return_date-r.rental_date) rent from category c \
join film_category fc on fc.category_id  = c.category_id \
join inventory i on i.film_id = fc.film_id \
join rental r on r.inventory_id = i.inventory_id \
join customer c2 on c2.customer_id =r.customer_id \
join address a on a.address_id = c2.address_id \
join city c3 on c3.city_id =a.city_id and c3.city like 'a%' \
group by name \
order by rent desc \
limit 1) \
union \
(select name, sum(r.return_date-r.rental_date) rent from category c \
join film_category fc on fc.category_id  = c.category_id \
join inventory i on i.film_id = fc.film_id \
join rental r on r.inventory_id = i.inventory_id \
join customer c2 on c2.customer_id =r.customer_id \
join address a on a.address_id = c2.address_id \
join city c3 on c3.city_id =a.city_id and c3.city like '%-%' \
group by name \
order by rent desc \
limit 1))").show()


