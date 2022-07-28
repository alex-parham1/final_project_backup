-- thirstee.cards definition

CREATE TABLE cards (
card_id NUMBER NOT NULL AUTOINCREMENT,
card_number TEXT NOT NULL,
PRIMARY KEY (card_id)
);

-- thirstee.customers definition

CREATE TABLE customers (
customer_id NUMBER NOT NULL AUTOINCREMENT,
name TEXT NOT NULL,
PRIMARY KEY (customer_id)
);
-- thirstee.products definition

CREATE TABLE products (
product_id NUMBER NOT NULL AUTOINCREMENT,
name TEXT NOT NULL,
flavour TEXT DEFAULT NULL,
size TEXT DEFAULT NULL,
price TEXT DEFAULT NULL,
PRIMARY KEY (product_id)
);

-- thirstee.store definition

CREATE TABLE store (
store_id NUMBER NOT NULL AUTOINCREMENT,
name TEXT NOT NULL,
PRIMARY KEY (store_id)
);

-- thirstee.transactions definition

CREATE TABLE transactions (
transaction_id NUMBER NOT NULL AUTOINCREMENT,
date_time DATETIME NOT NULL,
customer_id NUMBER NOT NULL REFERENCES CUSTOMERS(customer_id),
store_id NUMBER NOT NULL REFERENCES STORE(store_id),
total DECIMAL(10,2) NOT NULL,
payment_method TEXT DEFAULT 'CASH',
PRIMARY KEY (transaction_id));

-- thirstee.basket definition

CREATE TABLE basket (
transaction_id NUMBER NOT NULL REFERENCES TRANSACTIONS(transaction_id),
product_id NUMBER NOT NULL REFERENCES PRODUCTS(product_id));