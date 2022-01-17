DROP TABLE IF EXISTS market;
DROP TABLE IF EXISTS location;
DROP TABLE IF EXISTS comodity_price;


CREATE TABLE IF NOT EXISTS location (
	locality_id INT,
	locality_name VARCHAR(100),
	country_name VARCHAR(100),
	PRIMARY KEY(locality_id)
);


CREATE TABLE IF NOT EXISTS market (
	market_id INT,
	market_name VARCHAR(100) NOT NULL,
	locality_id INT,
	market_type VARCHAR(10),
	PRIMARY KEY(market_id),
	CONSTRAINT fk_location
		FOREIGN KEY (locality_id)
			REFERENCES location(locality_id)
			ON DELETE CASCADE;
);


CREATE TABLE IF NOT EXISTS comodity_price (
	comodity VARCHAR(100),
	year INT,
	month INT,
	price NUMERIC(18, 16),
	currency VARCHAR(3),
	units VARCHAR(10),
	PRIMARY KEY(comodity, year, month)
);


