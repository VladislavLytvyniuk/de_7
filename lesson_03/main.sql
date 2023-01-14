/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
SELECT 
    c.name category_name,
    count(*) AS films_count
FROM film 
JOIN film_category using(film_id)         
JOIN category c using(category_id)
GROUP BY category_name
ORDER BY films_count desc;
/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
SELECT 
    concat(a.first_name, ' ', a.last_name) AS actor_name,
    count(r.rental_id) rental_count
FROM rental r
JOIN inventory i using(inventory_id)
JOIN film_actor fa using (film_id)
JOIN actor a using(actor_id)
GROUP BY actor_name
ORDER BY rental_count desc
LIMIT 10;
/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
-- SQL code goes here...
SELECT 
    c.name AS top_category,
    sum(p.amount) as paid
FROM payment p
JOIN rental r using(rental_id)
JOIN inventory i using(inventory_id)
JOIN film_category fc using(film_id)
JOIN category c using(category_id)
GROUP BY top_category
ORDER BY paid DESC
LIMIT 1;
/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
-- SQL code goes here...
SELECT DISTINCT f.title
FROM film f
LEFT JOIN inventory i using(film_id)
WHERE i.film_id is null ;
/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...
SELECT 
    concat(a.first_name, ' ', a.last_name) AS actors,
    count(*) _cnt_category
FROM film f
JOIN film_actor fa using(film_id)
JOIN actor a using(actor_id)
JOIN film_category fc using(film_id)
JOIN category c using(category_id)
WHERE c.name = 'Children'
GROUP BY actors
ORDER BY _cnt_category desc
LIMIT 3;