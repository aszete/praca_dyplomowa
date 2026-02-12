-- address
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
