CREATE table IF NOT EXISTS customers (
	customer_id INT AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(255) NOT NULL
);

CREATE table IF NOT EXISTS products (
	product_id INT AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(255) NOT NULL
);

CREATE table IF NOT EXISTS store (
	store_id INT AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(255) NOT NULL
);

CREATE table IF NOT EXISTS cards (
	card_id INT AUTO_INCREMENT PRIMARY KEY,
	card_number BIGINT NOT NULL,
	card_type VARCHAR(255) NOT NULL
);

CREATE table IF NOT EXISTS cards_customers (
	customer_id INT NOT NULL,
	card_id INT NOT NULL,
	FOREIGN KEY (customer_id) 
		REFERENCES customers (customer_id),
	FOREIGN KEY (card_id) 
		REFERENCES cards (card_id)
);

CREATE table IF NOT EXISTS transactions (
	transaction_id INT AUTO_INCREMENT PRIMARY KEY,
	date_time DATETIME NOT NULL,
	customer_id INT NOT NULL,
	basket_id INT NOT NULL,
	quantity INT NOT NULL,
	store_id INT NOT NULL,
	total DECIMAL(10,2) NOT NULL,
	payment_method VARCHAR(255) DEFAULT 'CASH',
	
	FOREIGN KEY (customer_id)
		REFERENCES customers (customer_id),
	FOREIGN KEY (basket_id)
		REFERENCES basket (basket_id),
	FOREIGN KEY (store_id)
		REFERENCES store (store_id)
);
	
CREATE table IF NOT EXISTS basket (
	basket_id INT AUTO_INCREMENT PRIMARY KEY,
	transaction_id INT NOT NULL,
	product_id INT NOT NULL,

	FOREIGN KEY (transaction_id)
		REFERENCES transactions (transaction_id),
	FOREIGN KEY (product_id)
		REFERENCES products (product_id)
);
