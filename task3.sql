--1
select category, count(*) as film_count from film_list fl 
group by category


--2
select actor_id, (
select sum(rental_duration) from film f
join film_actor fa on f.film_id=fa.film_id
where fa.actor_id = a.actor_id
group by fa.actor_id
) as rent_sum
from actor a
order by rent_sum desc 
limit 10


--3
select distinct category, sum(price) over(partition by category) from film_list fl 
order by sum desc
limit 1

--4
select distinct title from film f
join inventory i on f.film_id = i.film_id 
where exists (select film_id from inventory) 


--5


/*select fa.actor_id, a.first_name, a.last_name, count(fa.actor_id) as count_films,
dense_rank() over(order by count(fa.actor_id) desc) from film_actor fa 
join (
select fid from film_list
where category = 'Children') fl on fa.film_id = fl.fid
join actor a on a.actor_id=fa.actor_id
group by fa.actor_id, a.first_name, a.last_name*/ 


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



/*select city, count(c.customer_id) as total, count(c.active) over(partition by city) from customer c 
join (select c2.city, a.address_id  from city c2, address a 
group by c2.city_id,a.address_id
having c2.city_id = a.city_id) с3 on c.address_id = с3.address_id
group by city,c.active*/


--7
/*with result_1 as(
select city, f.rental_duration  from city c 
join address a on a.city_id = c.city_id
join customer c1 on c1.address_id=a.address_id
join store s on s.store_id = c1.store_id
join inventory i on i.store_id = s.store_id
join film f on f.film_id = i.film_id
join film_category fc on f.film_id = fc.film_id
join category c2 on c2.category_id = fc.category_id and name like 'A%'),
result_2 as(
select city, f.rental_duration from city c
join address a on a.city_id = c.city_id
join customer c1 on c1.address_id=a.address_id
join store s on s.store_id = c1.store_id
join inventory i on i.store_id = s.store_id
join film f on f.film_id = i.film_id  
join film_category fc on f.film_id = fc.film_id
join category c2 on c2.category_id = fc.category_id
where city like '%-%')
select city, sum(rental_duration) as rent from result_1
group by city
order by rent desc*/

/*select name,total as rent from(
select name, sum(f.rental_duration) total  from city c 
join address a on a.city_id = c.city_id
join customer c1 on c1.address_id=a.address_id
join store s on s.store_id = c1.store_id
join inventory i on i.store_id = s.store_id
join film f on f.film_id = i.film_id
join film_category fc on f.film_id = fc.film_id
join category c2 on c2.category_id = fc.category_id and name like 'A%'
group by name 
union
select name, sum(f.rental_duration) total from city c
join address a on a.city_id = c.city_id
join customer c1 on c1.address_id=a.address_id
join store s on s.store_id = c1.store_id
join inventory i on i.store_id = s.store_id
join film f on f.film_id = i.film_id  
join film_category fc on f.film_id = fc.film_id
join category c2 on c2.category_id = fc.category_id
where city like '%-%'
group by name)
order by rent*/

/*select name,total as rent from(
select name, sum(r.return_date-r.rental_date) total  from city c 
join address a on a.city_id = c.city_id
join customer c1 on c1.address_id=a.address_id
join store s on s.store_id = c1.store_id
join inventory i on i.store_id = s.store_id
join film_category fc on i.film_id = fc.film_id
join category c2 on c2.category_id = fc.category_id and name like 'A%'
join rental r on r.inventory_id = i.inventory_id
group by name 
union
select name, sum(r.return_date-r.rental_date) total from city c
join address a on a.city_id = c.city_id
join customer c1 on c1.address_id=a.address_id
join store s on s.store_id = c1.store_id
join inventory i on i.store_id = s.store_id
join film_category fc on i.film_id = fc.film_id
join rental r on r.inventory_id = i.inventory_id
join category c2 on c2.category_id = fc.category_id
where city like '%-%'
group by name)
order by rent



select count(city)  from city c 
join address a on a.city_id = c.city_id
join customer c1 on c1.address_id=a.address_id
join store s on s.store_id = c1.store_id grouping i.store_id
join inventory i on i.store_id = s.store_id */
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