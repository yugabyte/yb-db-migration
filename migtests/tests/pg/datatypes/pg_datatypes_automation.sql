-- datatypes 

drop table if exists num_types;

create table num_types(v1 smallint, v2 integer,v3 bigint,v4 decimal(6,3),v5 numeric, v6 money);

insert into num_types values(182,34453,654385451,453.23,22334.542,120.50);
insert into num_types values(32762,-3415123,654312385451,999.999,-22334.542,10.4);
insert into num_types values(-323,53,-90654385451,-459.230,9992334.54290,-12000500.50);

\d num_types

select * from num_types;

drop type if exists week cascade;

CREATE TYPE week AS ENUM ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun');

drop table if exists datatypes1;

create table datatypes1(bool_type boolean,char_type1 CHAR (1),varchar_type VARCHAR(100),byte_type bytea, enum_type week);

insert into datatypes1 values(true,'z','this is a string','01010','Mon');
insert into datatypes1 values(false,'5','Lorem ipsum dolor sit amet, consectetuer adipiscing elit.','-abcd','Fri');
insert into datatypes1 values(true,'z','this is a string','4458','Sun');

\d datatypes1

select * from datatypes1;

drop table if exists datetime_type;

create table datetime_type(v1 date, v2 time, v3 timestamp,v4 TIMESTAMP without TIME ZONE default CURRENT_TIMESTAMP(0));

insert into datetime_type values('1996-12-02', '09:00:00',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP(0));
insert into datetime_type values('2006-12-02', '12:00:50','2022-11-01 15:55:58.091241',CURRENT_TIMESTAMP(0));
insert into datetime_type values('1992-01-23', null,current_timestamp,'2022-11-01 15:58:02');

\d datetime_type

select * from datetime_type;

drop table if exists datetime_type2;

create table datetime_type2(v1 timestamp);
insert into datetime_type2 values('2022-11-01 15:55:58.091241');
insert into datetime_type2 values('2022-11-01 15:58:02');

\d datetime_type2

select * from datetime_type2;

drop table if exists datatypes2;

create table datatypes2(v1 json, v2 BIT(10), v3 int ARRAY[4], v4 text[][]);

insert into datatypes2 values ('{"key1": "value1", "key2": "value2"}',B'10'::bit(10),'{20000, 14600, 23500, 13250}', '{{“FD”, “MF”}, {“FD”, “Property”}}');
insert into datatypes2 values ('["a","b","c",1,2,3]',B'100011'::bit(10),'{20000, 14600, 23500, 13250}', '{{“FD”, “MF”}, {"act","two"}}');
insert into datatypes2 values (null,B'1'::bit(10),null, '{{“FD”}, {"act"}}');

\d datatypes2

select * from datatypes2;

