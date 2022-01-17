DO $$
DECLARE
	max_id int := (SELECT MAX(location_id) FROM location);
BEGIN
	FOR i IN 1..10 LOOP
		INSERT INTO location(location_id, locality_name, country_name)
			VALUES (max_id+i, NULL, 'Republic of Mars');
	END LOOP;
END;
$$
