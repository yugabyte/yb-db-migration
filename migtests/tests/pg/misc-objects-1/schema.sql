-- contains Types, Domains, Comments, Mixed Cases, WITH clause examples

DROP type IF EXISTS enum_kind cascade; 

CREATE TYPE enum_kind AS ENUM (
    'YES',
    'NO',
    'UNKNOWN'
);


DROP type IF EXISTS Item_details cascade;

CREATE TYPE Item_details AS (  
    item_id INT,  
    item_name VARCHAR,  
    item_price Numeric(5,2)  
);  

CREATE TABLE orders (
    item Item_details,
    number_of_items int,
    created_at timestamp with time zone default now()
);

INSERT INTO orders (item, number_of_items) VALUES (ROW(1, 'Shoes', 100.00), 5); 
INSERT INTO orders (item, number_of_items) VALUES (ROW(2, 'Shirts', 50.00), 10);
INSERT INTO orders (item, number_of_items) VALUES (ROW(3, 'Pants', 75.00), 6);

CREATE TABLE products (
    item Item_details,
    added_at timestamp with time zone default now(),
    updated_at timestamp with time zone default now()
);

INSERT INTO products (item) VALUES (ROW(1, 'Shoes', 100.00)); 
INSERT INTO products (item) VALUES (ROW(2, 'Shirts', 50.00));
INSERT INTO products (item) VALUES (ROW(3, 'Pants', 75.00));

DROP DOMAIN IF EXISTS person_name cascade; 

CREATE DOMAIN person_name AS   
VARCHAR NOT NULL CHECK (value!~ '\s'); 

drop table if exists "Recipients";

CREATE TABLE "Recipients" (  
ID SERIAL PRIMARY KEY,  
    First_name person_name,  
    Last_name person_name,  
    Misc enum_kind  
    );  
    
insert into "Recipients"(First_name,Last_name,Misc) values ('abc','xyz','YES');

drop table if exists session_log;

create table session_log 
( 
   userid int not null, 
   phonenumber int
); 

comment on column session_log.userid is 'The user ID';
comment on column session_log.phonenumber is 'The phone number including the area code';

comment on table session_log is 'Our session logs';

\d+ session_log

\dt+ session_log

create table session_log1 
( 
   userid int not null, 
   phonenumber int
); 

comment on column session_log1.userid is 'The user ID for log1';
comment on column session_log1.phonenumber is 'The phone number including the area code for log1';

comment on table session_log1 is 'Our session logs1';

\d+ session_log

\dt+ session_log

create table session_log2 
( 
   userid int not null, 
   phonenumber int
); 

comment on column session_log2.userid is 'The user ID for log2';
comment on column session_log2.phonenumber is 'The phone number including the area code for log2';

comment on table session_log2 is 'Our session logs2';

\d+ session_log

\dt+ session_log


do $$
begin
   for i in 1..100 loop
      INSERT INTO session_log VALUES(i,i);
      INSERT INTO session_log1 VALUES(i,i);
      INSERT INTO session_log2 VALUES(i,i);
   end loop;
end; $$;

drop table if exists "Mixed_Case_Table_Name_Test";

create table "Mixed_Case_Table_Name_Test" (
	id serial primary key,
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	email VARCHAR(50),
	gender VARCHAR(50),
	ip_address VARCHAR(20)
    
);

-- aut int NOT NULL GENERATED ALWAYS AS ((id*9)+1) stored

insert into "Mixed_Case_Table_Name_Test" (first_name, last_name, email, gender, ip_address) values ('Modestine', 'MacMeeking', 'mmacmeeking0@zimbio.com', 'Female', '208.44.58.185');
insert into "Mixed_Case_Table_Name_Test" (first_name, last_name, email, gender, ip_address) values ('Genna', 'Kaysor', 'gkaysor1@hibu.com', 'Female', '202.48.51.58');
insert into "Mixed_Case_Table_Name_Test" (first_name, last_name, email, gender, ip_address) values ('Tess', 'Wesker', 'twesker2@scientificamerican.com', 'Female', '177.153.32.186');
insert into "Mixed_Case_Table_Name_Test" (first_name, last_name, email, gender, ip_address) values ('Magnum', 'Danzelman', 'mdanzelman3@storify.com', 'Bigender', '192.200.33.56');
insert into "Mixed_Case_Table_Name_Test" (first_name, last_name, email, gender, ip_address) values ('Mitzi', 'Pidwell', 'mpidwell4@shutterfly.com', 'Female', '216.4.250.71');

\d "Mixed_Case_Table_Name_Test"

select * from "Mixed_Case_Table_Name_Test";

drop table if exists "Case_Sensitive_Columns";

create table "Case_Sensitive_Columns" (
	id serial primary key,
	"user" VARCHAR(50),
	"Last_Name" VARCHAR(50),
	email VARCHAR(50),
	gender VARCHAR(50),
	ip_address VARCHAR(20)
    
);

insert into "Case_Sensitive_Columns" ("user", "Last_Name", email, gender, ip_address) values ('Modestine', 'MacMeeking', 'mmacmeeking0@zimbio.com', 'Female', '208.44.58.185');
insert into "Case_Sensitive_Columns" ("user", "Last_Name", email, gender, ip_address) values ('Genna', 'Kaysor', 'gkaysor1@hibu.com', 'Female', '202.48.51.58');
insert into "Case_Sensitive_Columns" ("user", "Last_Name", email, gender, ip_address) values ('Tess', 'Wesker', 'twesker2@scientificamerican.com', 'Female', '177.153.32.186');
insert into "Case_Sensitive_Columns" ("user", "Last_Name", email, gender, ip_address) values ('Magnum', 'Danzelman', 'mdanzelman3@storify.com', 'Bigender', '192.200.33.56');
insert into "Case_Sensitive_Columns" ("user", "Last_Name", email, gender, ip_address) values ('Mitzi', 'Pidwell', 'mpidwell4@shutterfly.com', 'Female', '216.4.250.71');

\d "Case_Sensitive_Columns"

select * from "Case_Sensitive_Columns";


-- Different cases with the WITH clause in the tables

-- Table name and column name is "WITH"

CREATE TABLE "WITH" (
    id SERIAL PRIMARY KEY,
    "WITH" VARCHAR(100)  /* Column name is "WITH" */
)
WITH (
    FILLFACTOR = 75,  /* Fill factor set to 75% */
    autovacuum_enabled = TRUE,  /* Enable autovacuum */
    autovacuum_analyze_scale_factor = 0.05  /* Scale factor for analyze */
);

-- Insert statements for the "WITH" table
INSERT INTO "WITH" ("WITH") VALUES ('Value 1');
INSERT INTO "WITH" ("WITH") VALUES ('Value 2');
INSERT INTO "WITH" ("WITH") VALUES ('Value 3');


-- WITH clause in comments variation 1

CREATE TABLE with_example1 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) -- WITH()
)
/* 
WITH (
    FILLFACTOR = 80,  -- Active fill factor setting
    autovacuum_enabled = TRUE,  -- Autovacuum enabled
    autovacuum_vacuum_scale_factor = 0.1,   -- Scale factor for vacuum
    autovacuum_freeze_min_age = 10000000  -- Min age for freeze
    -- These settings are commented out and will not be applied
)
*/
WITH (
    FILLFACTOR = 80,  /* Fill factor: 70% 
                         Special characters: {}, (), [], @, # */
    autovacuum_enabled = TRUE,  /* Autovacuum enabled */
    autovacuum_vacuum_scale_factor = 0.1,  /* Scale factor for vacuum */
    autovacuum_freeze_min_age = 10000000  /* Min age for freeze */
    /* Multiline
       Comment
    */
);

-- Insert statements for the with_example1 table
INSERT INTO with_example1 (name) VALUES ('John Doe');
INSERT INTO with_example1 (name) VALUES ('Jane Smith');
INSERT INTO with_example1 (name) VALUES ('Alice Johnson');


-- WITH clause in comments variation 2

CREATE TABLE with_example2 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) /*
    WITH()
    */
)
WITH (
    FILLFACTOR = 80,  /* Fill factor: 70% 
                         Special characters: {}, (), [], @, # */
    autovacuum_enabled = TRUE,  /* Autovacuum enabled */
    autovacuum_vacuum_scale_factor = 0.1,  /* Scale factor for vacuum */
    autovacuum_freeze_min_age = 10000000  /* Min age for freeze */
    -- Comment at the end of the block
)
/* 
WITH (
    FILLFACTOR = 80,  -- Active fill factor setting
    autovacuum_enabled = TRUE,  -- Autovacuum enabled
    autovacuum_vacuum_scale_factor = 0.1,   -- Scale factor for vacuum
    autovacuum_freeze_min_age = 10000000,  -- Min age for freeze
)
*/
;

-- Insert statements for the with_example2 table
INSERT INTO with_example2 (name) VALUES ('Michael Brown');
INSERT INTO with_example2 (name) VALUES ('Emily Davis');
INSERT INTO with_example2 (name) VALUES ('William Wilson');



