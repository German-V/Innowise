--1 Вывести количество фильмов в каждой категории, отсортировать по убыванию.
select category, count(*) as film_count from film_list fl 
group by category
order by film_count desc


--2 Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
select actor_id, (
select sum(rental_duration) from film f
join film_actor fa on f.film_id=fa.film_id
where fa.actor_id = a.actor_id
group by fa.actor_id
) as rent_sum
from actor a
order by rent_sum desc 
limit 10


--3 Вывести категорию фильмов, на которую потратили больше всего денег.
select distinct category, sum(price) over(partition by category) sum from film_list fl 
order by sum desc
fetch first row with ties

--4 Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
select distinct title from film f
where not exists (select film_id from inventory i where f.film_id = i.film_id) 
--без подзапроса

select distinct title from film f
left join inventory i on f.film_id = i.film_id
where i.film_id is null




--5

select first_name, last_name, total from(
select dense_rank() over(order by count(a.actor_id) desc) dn, a.first_name, a.last_name, count(a.actor_id) as total  from actor a 
join film_actor fa on a.actor_id = fa.actor_id 
join film_category fc on fa.film_id = fc.film_id 
join category c on c.category_id =fc.category_id 
where c."name" = 'Children' 
group by a.first_name,a.last_name ) b
where dn <= 3


--6
select city, count(c2.customer_id), sum(c2.active) active, count(*) filter(where c2.active=0) inactive from city c 
left join address a on a.city_id = c.city_id 
join customer c2 ON a.address_id = c2.address_id
group by city
order by inactive desc


--7


select name, rent from((
select name, sum(r.return_date-r.rental_date) rent from category c 
join film_category fc on fc.category_id  = c.category_id 
join inventory i on i.film_id = fc.film_id 
join rental r on r.inventory_id = i.inventory_id 
join customer c2 on c2.customer_id =r.customer_id 
join address a on a.address_id = c2.address_id 
join city c3 on c3.city_id =a.city_id and c3.city like 'a%'
group by name
order by rent desc
limit 1)
union 
(select name, sum(r.return_date-r.rental_date) rent from category c 
join film_category fc on fc.category_id  = c.category_id 
join inventory i on i.film_id = fc.film_id 
join rental r on r.inventory_id = i.inventory_id 
join customer c2 on c2.customer_id =r.customer_id 
join address a on a.address_id = c2.address_id 
join city c3 on c3.city_id =a.city_id and c3.city like '%-%'
group by name
order by rent desc
limit 1))