-- contains Aggregates, Procedures, triggers, functions, extensions, inline comments


CREATE EXTENSION IF NOT EXISTS pgcrypto;

drop table if exists ext_test;

create table ext_test(id serial, password text);

insert into ext_test(password) values (crypt('johnspassword', gen_salt('bf')));

select * from ext_test;

drop aggregate if exists inc_sum(int);

CREATE AGGREGATE inc_sum(int) (
    sfunc = int4pl,
    stype = int,
    initcond = 10
);

select aggfnoid,agginitval,aggkind  from pg_aggregate
where aggfnoid = 'inc_sum'::regproc;


drop table if exists tt;

CREATE TABLE tt (i int);

drop table if exists audit;

create table audit(id text);

drop function if exists auditlogfunc();

CREATE OR REPLACE FUNCTION auditlogfunc() RETURNS TRIGGER AS $example_table$
   BEGIN
      INSERT INTO AUDIT(id) VALUES (current_timestamp); -- random comment
      RETURN NEW;
   END;
$example_table$ LANGUAGE plpgsql;

drop trigger if exists audit_trigger on tt;

CREATE TRIGGER audit_trigger AFTER INSERT ON tt
FOR EACH ROW EXECUTE PROCEDURE auditlogfunc();



drop procedure if exists tt_insert_data;

CREATE OR REPLACE PROCEDURE tt_insert_data("i" integer)

LANGUAGE SQL

AS $$

INSERT INTO public."tt" VALUES ("i");

$$;

CALL tt_insert_data(1);
CALL tt_insert_data(2);
CALL tt_insert_data(3);
CALL tt_insert_data(4);

CREATE OR REPLACE FUNCTION total ()
RETURNS integer AS $agg_output$
/******************************************************************************
  PACKAGE NAME :  fnc_req_ansr_delete
  DESCRIPTION:    This function return Request ID, Reason ID, along with JSON structure of Repetition_N

  REVISION HISTORY
  Date          Modified By         Description
  ----------    -----------         -----------------------------------------
  18/05/2018    Saurabh Singh  		Initial implementation.
  
	Input Parms:
    1. pv_in_per_id     -- WID (mandatory)                		--the function parameter type: rs_req_dbo.req.per_id%TYPE
    2. pv_in_pln_id     -- Plan Identifier (mandatory)    		--the function parameter type: rs_req_dbo.req.pln_id%TYPE
    3. pv_in_wi_req_id  -- Request Identifier (mandatory) 		--the function parameter type: rs_req_dbo.req_ansr.wi_req_id%TYPE,
    4. pv_in_rsn_id 		-- Reason ID Passed by API [REQUIRED]	--the function parameter type: rs_req_dbo.req_ansr.rsn_id%TYPE
    5. pv_in_json			-- JSON arry containing Repetition_N
	6. pv_in_req_src_x		jsonb		All logging information
	
	Output Parms:
	1. pv_out_json 		-- 	Return JSON Array of Repetition_N as rsc_id and rec_upd_tmst: 
    							{"rsc_id": null, "rec_upd_tmst": "2018-06-07T20:20:00"}

  
  29/05/2018	Saurabh Singh		Removed Return Code and Message from the function
  17/07/2018    Eoin Kelly          Implemented changes to JSON output and input params.
  08/08/2018    Saurabh Singh       Implemented New logic for Null Reason ID and null jason to delete based on what is provided.
  
  Sample Input:
  Select * from rs_req_fnc_dbo.fnc_req_ansr_delete(
  '{"api_appl_id":"default","api_lg_trk_id":"default","api_usr_role_c":"default","api_usr_id_ty_c":"default","api_usr_id":"default","api_read_only_i":"default"}',
  123456, 25001, 2, 2, '{"repitition_numbers" : [{"repitition_n" : 2}]}');
  
  Sample Output:
  {"rsc_id": null, "rec_upd_tmst": "2018-06-07T20:20:00"}    --- rsc_id is the REPITITION NUMBER
  
  ******************************************************************************/
declare
	total integer;
BEGIN
   SELECT inc_sum(i) into total FROM tt;
   RETURN total;
END;
$agg_output$ LANGUAGE plpgsql;

select total();
SELECT * from audit;




create table c(id int, c text, nc text);

insert into c values(1,'abc','abc');
insert into c values(2,'\a','\a'); 
insert into c values(3,'null','NULL');
insert into c values(4,'
','tt');
insert into c values(5,'	','abc');
insert into c values(6,'abc	abc','abc'); 
insert into c values(7,'abc
abc','abc');
insert into c values(8,'"abc"','ab"');
insert into c values(9,'"abc"','"abcn''"');
insert into c values(10,'abc abc','abc abc'); 
insert into c values(11,NULL,'abc');
insert into c values(12,'abc',null);

--------------------------------------------------
-- Carriage return test
CREATE TABLE foo (
    id   INTEGER PRIMARY KEY,
    value TEXT
);

INSERT INTO foo (id, value) VALUES (1, E'\r\nText with \r');
INSERT INTO foo (id, value) VALUES (2, E'\r\nText with \n');
INSERT INTO foo (id, value) VALUES (3, E'\r\nText with \r\n');
INSERT INTO foo (id, value) VALUES (4, E'\r\nText with \\r');
INSERT INTO foo (id, value) VALUES (5, E'\r\nText with \\n');
INSERT INTO foo (id, value) VALUES (6, E'\r\nText with \\r\\n');

INSERT INTO foo (id, value) VALUES (7, E'Text with \r\n');
INSERT INTO foo (id, value) VALUES (8, E'Text with \r\nText with \r');
INSERT INTO foo (id, value) VALUES (9, E'Text with \r\nText with \n');
INSERT INTO foo (id, value) VALUES (10, E'Text with \r\nText with \r\n');
INSERT INTO foo (id, value) VALUES (11, E'Text with \r\nText with \\r');
INSERT INTO foo (id, value) VALUES (12, E'Text with \r\nText with \\n');
INSERT INTO foo (id, value) VALUES (13, E'Text with \r\nText with \\r\\n');

INSERT INTO foo (id, value) VALUES (14, E'Text with \rText with \r\n');
INSERT INTO foo (id, value) VALUES (15, E'Text with \rText with \n');
INSERT INTO foo (id, value) VALUES (16, E'Text with \rText with \r\n');
INSERT INTO foo (id, value) VALUES (17, E'Text with \rText with \\r');
INSERT INTO foo (id, value) VALUES (18, E'Text with \rText with \\n');
INSERT INTO foo (id, value) VALUES (19, E'Text with \rText with \\r\\n');

INSERT INTO foo (id, value) VALUES (20, E'Text with \nText with \r');
INSERT INTO foo (id, value) VALUES (21, E'Text with \nText with \n');
INSERT INTO foo (id, value) VALUES (22, E'Text with \nText with \r\n');
INSERT INTO foo (id, value) VALUES (23, E'Text with \nText with \\r');
INSERT INTO foo (id, value) VALUES (24, E'Text with \nText with \\n');
INSERT INTO foo (id, value) VALUES (25, E'Text with \nText with \\r\\n');

INSERT INTO foo (id, value) VALUES (26, E'Text with \r\nText with \rText with \r\n');
INSERT INTO foo (id, value) VALUES (27, E'Text with \r\nText with \nText with \r\n');
INSERT INTO foo (id, value) VALUES (28, E'Text with \r\nText with \r\nText with \r\n');
INSERT INTO foo (id, value) VALUES (29, E'Text with \r\nText with \\rText with \r\n');
INSERT INTO foo (id, value) VALUES (30, E'Text with \r\nText with \\nText with \r\n');
INSERT INTO foo (id, value) VALUES (31, E'Text with \r\nText with \\r\\nText with \r\n');

INSERT INTO foo (id, value) VALUES (32, E'\n\rText with \r');
INSERT INTO foo (id, value) VALUES (33, E'Text with \n\r');
INSERT INTO foo (id, value) VALUES (34, E'Text with \r\nText with \n\r');


CREATE TABLE cities (
    name       TEXT,
    population REAL,
    elevation  INT
);
 
CREATE TABLE capitals (
    state CHAR(2) NOT NULL
) INHERITS (cities);

-- Inserting into the parent table
INSERT INTO cities (name, population, elevation) VALUES 
('San Francisco', 884363, 16),
('Denver', 704621, 1609),
('Los Angeles', 3980400, 71),
('Chicago', 2716000, 594),
('Miami', 467963, 2),
('Seattle', 744955, 184),
('Boston', 694583, 43);


-- Inserting into the child table (capital)
INSERT INTO capitals (name, population, elevation, state) VALUES 
('Sacramento', 508529, 9, 'CA'),
('Denver', 704621, 1609, 'CO'),
('Phoenix', 1660272, 331, 'AZ'),
('Austin', 964254, 149, 'TX'),
('Tallahassee', 191049, 62, 'FL'),
('Olympia', 52389, 95, 'WA'),
('Boston', 694583, 43, 'MA');
