CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT EXISTS final AUTHORIZATION postgres;

-- Final
CREATE TABLE final.dim_customer(
    customer_id uuid primary key default uuid_generate_v4(),
    customer_nk int,
    first_name varchar(100),
    last_name varchar(100),
    email varchar(100),
    phone varchar(100),
    address varchar(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE final.dim_product(
    product_id uuid primary key default uuid_generate_v4(),
    product_nk varchar(100),
    name text,
    price numeric(10,2),
    stock int,
    category_name varchar(255),
    category_desc text,
    subcategory_name varchar(255),
    subcategory_desc text,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE final.dim_date(
    date_id              	 INT NOT null primary KEY,
    date_actual              DATE NOT NULL,
    day_suffix               VARCHAR(4) NOT NULL,
    day_name                 VARCHAR(9) NOT NULL,
    day_of_year              INT NOT NULL,
    week_of_month            INT NOT NULL,
    week_of_year             INT NOT NULL,
    week_of_year_iso         CHAR(10) NOT NULL,
    month_actual             INT NOT NULL,
    month_name               VARCHAR(9) NOT NULL,
    month_name_abbreviated   CHAR(3) NOT NULL,
    quarter_actual           INT NOT NULL,
    quarter_name             VARCHAR(9) NOT NULL,
    year_actual              INT NOT NULL,
    first_day_of_week        DATE NOT NULL,
    last_day_of_week         DATE NOT NULL,
    first_day_of_month       DATE NOT NULL,
    last_day_of_month        DATE NOT NULL,
    first_day_of_quarter     DATE NOT NULL,
    last_day_of_quarter      DATE NOT NULL,
    first_day_of_year        DATE NOT NULL,
    last_day_of_year         DATE NOT NULL,
    mmyyyy                   CHAR(6) NOT NULL,
    mmddyyyy                 CHAR(10) NOT NULL,
    weekend_indr             VARCHAR(20) NOT NULL
);

CREATE TABLE final.fct_order(
	order_id uuid default uuid_generate_v4() ,
	product_id uuid references final.dim_product(product_id),
	customer_id uuid references final.dim_customer(customer_id),
	date_id int references final.dim_date(date_id),
	quantity int,
	status varchar(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_unique_order_product ON final.fct_order (order_id, product_id, customer_id, date_id, quantity, status, created_at);

-- Add Unique Constraints to fact tables
ALTER TABLE final.fct_order
ADD CONSTRAINT fct_order_unique UNIQUE (order_id);