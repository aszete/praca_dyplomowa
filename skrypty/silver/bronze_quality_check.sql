-- address
-- check for duplicates in IDs
SELECT		address_id, COUNT(*)
FROM		bronze.customers
GROUP BY	address_id
HAVING count(*) > 1;

SELECT		country
FROM		bronze.addresses
WHERE		city != TRIM(city);

SELECT		city
FROM		bronze.addresses
WHERE		city != TRIM(city);

SELECT		street
FROM		bronze.addresses
WHERE		street != TRIM(street);

SELECT		postal_code
FROM		bronze.addresses
WHERE		city != TRIM(city);

-- customer
-- check for duplicates in IDs
SELECT		customer_id, COUNT(*)
FROM		bronze.customers
GROUP BY	customer_id
HAVING count(*) > 1;

SELECT		address_id, COUNT(*)
FROM		bronze.customers
GROUP BY	address_id
HAVING count(*) > 1;

SELECT		first_name
FROM		bronze.customers
WHERE		first_name != TRIM(first_name);

SELECT		last_name
FROM		bronze.customers
WHERE		last_name != TRIM(last_name);

SELECT		email
FROM		bronze.customers
WHERE		email != TRIM(email);

SELECT		gender, COUNT(*)
FROM		bronze.customers
GROUP BY	gender;

--categories
-- Check for duplicated in IDs
SELECT		category_id, COUNT(*)
FROM		bronze.categories
GROUP BY	category_id
HAVING count(*) > 1;

--brands
-- Check for duplicated in IDs
SELECT		brand_id, COUNT(*)
FROM		bronze.brands
GROUP BY	brand_id
HAVING count(*) > 1;

SELECT		brand_id, COUNT(*)
FROM		  bronze.brands
GROUP BY	brand_id;

--payment methods
-- Check for duplicated in IDs
SELECT		payment_method_id, COUNT(*)
FROM		bronze.payment_methods
GROUP BY	payment_method_id
HAVING count(*) > 1;

SELECT		payment_method_id, COUNT(*)
FROM		  bronze.payment_methods
GROUP BY	payment_method_id;

