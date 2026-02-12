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
