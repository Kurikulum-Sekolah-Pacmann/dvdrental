-- DROP SCHEMA public;

CREATE SCHEMA dvdrental AUTHORIZATION pg_database_owner;


CREATE TABLE dvdrental.actor (
	actor_id serial4 NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) NOT NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT actor_pkey PRIMARY KEY (actor_id)
);
CREATE INDEX idx_actor_last_name ON dvdrental.actor USING btree (last_name);


CREATE TABLE dvdrental.address (
	address_id serial4 NOT NULL,
	address varchar(50) NULL,
	address2 varchar(50) NULL,
	district varchar(20) NULL,
	city_id int2 NULL,
	postal_code varchar(10) NULL,
	phone varchar(20) NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT address_pkey PRIMARY KEY (address_id)
);
CREATE INDEX idx_fk_city_id ON dvdrental.address USING btree (city_id);


CREATE TABLE dvdrental.category (
	category_id serial4 NOT NULL,
	"name" varchar(25) NOT NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT category_pkey PRIMARY KEY (category_id)
);

CREATE TABLE dvdrental.city (
	city_id serial4 NOT NULL,
	city varchar(50) NOT NULL,
	country_id int2 NOT NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT city_pkey PRIMARY KEY (city_id)
);
CREATE INDEX idx_fk_country_id ON dvdrental.city USING btree (country_id);

CREATE TABLE dvdrental.country (
	country_id serial4 NOT NULL,
	country varchar(50) NOT NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT country_pkey PRIMARY KEY (country_id)
);

CREATE TABLE dvdrental.customer (
	customer_id serial4 NOT NULL,
	store_id int2 NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) NOT NULL,
	email varchar(50) NULL,
	address_id int2 NOT NULL,
	activebool bool DEFAULT true NOT NULL,
	create_date date DEFAULT 'now'::text::date NOT NULL,
	last_update timestamp DEFAULT now() NULL,
	active int4 NULL,
	CONSTRAINT customer_pkey PRIMARY KEY (customer_id)
);
CREATE INDEX idx_fk_address_id ON dvdrental.customer USING btree (address_id);
CREATE INDEX idx_fk_store_id ON dvdrental.customer USING btree (store_id);
CREATE INDEX idx_last_name ON dvdrental.customer USING btree (last_name);

CREATE TABLE dvdrental.film (
	film_id serial4 NOT NULL,
	title varchar(255) NOT NULL,
	description text NULL,
	release_year int2 NULL,
	language_id int2 NOT NULL,
	rental_duration int2 DEFAULT 3 NOT NULL,
	rental_rate numeric(4, 2) DEFAULT 4.99 NOT NULL,
	length int2 NULL,
	replacement_cost numeric(5, 2) DEFAULT 19.99 NOT NULL,
	rating varchar(50) NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	special_features text NULL,
	fulltext text NOT NULL,
	CONSTRAINT film_pkey PRIMARY KEY (film_id)
);
CREATE INDEX idx_fk_language_id ON dvdrental.film USING btree (language_id);
CREATE INDEX idx_title ON dvdrental.film USING btree (title);

CREATE TABLE dvdrental.film_actor (
	actor_id int2 NOT NULL,
	film_id int2 NOT NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT film_actor_pkey PRIMARY KEY (actor_id, film_id)
);
CREATE INDEX idx_fk_film_id ON dvdrental.film_actor USING btree (film_id);

CREATE TABLE dvdrental.film_category (
	film_id int2 NOT NULL,
	category_id int2 NOT NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT film_category_pkey PRIMARY KEY (film_id, category_id)
);

CREATE TABLE dvdrental.inventory (
	inventory_id serial4 NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT inventory_pkey PRIMARY KEY (inventory_id)
);
CREATE INDEX idx_store_id_film_id ON dvdrental.inventory USING btree (store_id, film_id);

CREATE TABLE dvdrental."language" (
	language_id serial4 NOT NULL,
	"name" bpchar(20) NOT NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT language_pkey PRIMARY KEY (language_id)
);

CREATE TABLE dvdrental.payment (
	payment_id serial4 NOT NULL,
	customer_id int2 NOT NULL,
	staff_id int2 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5, 2) NOT NULL,
	payment_date timestamp NOT NULL,
	CONSTRAINT payment_pkey PRIMARY KEY (payment_id)
);
CREATE INDEX idx_fk_customer_id ON dvdrental.payment USING btree (customer_id);
CREATE INDEX idx_fk_rental_id ON dvdrental.payment USING btree (rental_id);
CREATE INDEX idx_fk_staff_id ON dvdrental.payment USING btree (staff_id);

CREATE TABLE dvdrental.rental (
	rental_id serial4 NOT NULL,
	rental_date timestamp NOT NULL,
	inventory_id int4 NOT NULL,
	customer_id int2 NOT NULL,
	return_date timestamp NULL,
	staff_id int2 NOT NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT rental_pkey PRIMARY KEY (rental_id)
);
CREATE INDEX idx_fk_inventory_id ON dvdrental.rental USING btree (inventory_id);
CREATE UNIQUE INDEX idx_unq_rental_rental_date_inventory_id_customer_id ON dvdrental.rental USING btree (rental_date, inventory_id, customer_id);

CREATE TABLE dvdrental.staff (
	staff_id serial4 NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) NOT NULL,
	address_id int2 NOT NULL,
	email varchar(50) NULL,
	store_id int2 NOT NULL,
	active bool DEFAULT true NOT NULL,
	username varchar(16) NOT NULL,
	"password" varchar(40) NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	picture bytea NULL,
	CONSTRAINT staff_pkey PRIMARY KEY (staff_id)
);

CREATE TABLE dvdrental.store (
	store_id serial4 NOT NULL,
	manager_staff_id int2 NOT NULL,
	address_id int2 NOT NULL,
	last_update timestamp DEFAULT now() NOT NULL,
	CONSTRAINT store_pkey PRIMARY KEY (store_id)
);
CREATE UNIQUE INDEX idx_unq_manager_staff_id ON dvdrental.store USING btree (manager_staff_id);

ALTER TABLE ONLY dvdrental.customer
    ADD CONSTRAINT customer_address_id_fkey FOREIGN KEY (address_id) REFERENCES dvdrental.address(address_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.film_actor
    ADD CONSTRAINT film_actor_actor_id_fkey FOREIGN KEY (actor_id) REFERENCES dvdrental.actor(actor_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.film_actor
    ADD CONSTRAINT film_actor_film_id_fkey FOREIGN KEY (film_id) REFERENCES dvdrental.film(film_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.film_category
    ADD CONSTRAINT film_category_category_id_fkey FOREIGN KEY (category_id) REFERENCES dvdrental.category(category_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.film_category
    ADD CONSTRAINT film_category_film_id_fkey FOREIGN KEY (film_id) REFERENCES dvdrental.film(film_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.film
    ADD CONSTRAINT film_language_id_fkey FOREIGN KEY (language_id) REFERENCES dvdrental.language(language_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.address
    ADD CONSTRAINT fk_address_city FOREIGN KEY (city_id) REFERENCES dvdrental.city(city_id);

ALTER TABLE ONLY dvdrental.city
    ADD CONSTRAINT fk_city FOREIGN KEY (country_id) REFERENCES dvdrental.country(country_id);

ALTER TABLE ONLY dvdrental.inventory
    ADD CONSTRAINT inventory_film_id_fkey FOREIGN KEY (film_id) REFERENCES dvdrental.film(film_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.payment
    ADD CONSTRAINT payment_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES dvdrental.customer(customer_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.payment
    ADD CONSTRAINT payment_rental_id_fkey FOREIGN KEY (rental_id) REFERENCES dvdrental.rental(rental_id) ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE ONLY dvdrental.payment
    ADD CONSTRAINT payment_staff_id_fkey FOREIGN KEY (staff_id) REFERENCES dvdrental.staff(staff_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.rental
    ADD CONSTRAINT rental_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES dvdrental.customer(customer_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.rental
    ADD CONSTRAINT rental_inventory_id_fkey FOREIGN KEY (inventory_id) REFERENCES dvdrental.inventory(inventory_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.rental
    ADD CONSTRAINT rental_staff_id_key FOREIGN KEY (staff_id) REFERENCES dvdrental.staff(staff_id);

ALTER TABLE ONLY dvdrental.staff
    ADD CONSTRAINT staff_address_id_fkey FOREIGN KEY (address_id) REFERENCES dvdrental.address(address_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.store
    ADD CONSTRAINT store_address_id_fkey FOREIGN KEY (address_id) REFERENCES dvdrental.address(address_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY dvdrental.store
    ADD CONSTRAINT store_manager_staff_id_fkey FOREIGN KEY (manager_staff_id) REFERENCES dvdrental.staff(staff_id) ON UPDATE CASCADE ON DELETE RESTRICT;