
drop table if exists view_table1;

create table view_table1 (
	id int primary key auto_increment,
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	email VARCHAR(50),
	gender VARCHAR(50),
	ip_address VARCHAR(20)
);
insert into view_table1 (first_name, last_name, email, gender, ip_address) values ('Modestine', 'MacMeeking', 'mmacmeeking0@zimbio.com', 'Female', '208.44.58.185');
insert into view_table1 (first_name, last_name, email, gender, ip_address) values ('Genna', 'Kaysor', 'gkaysor1@hibu.com', 'Male', '202.48.51.58');
insert into view_table1 (first_name, last_name, email, gender, ip_address) values ('Tess', 'Wesker', 'twesker2@scientificamerican.com', 'Female', '177.153.32.186');
insert into view_table1 (first_name, last_name, email, gender, ip_address) values ('Magnum', 'Danzelman', 'mdanzelman3@storify.com', 'Bigender', '192.200.33.56');
insert into view_table1 (first_name, last_name, email, gender, ip_address) values ('Mitzi', 'Pidwell', 'mpidwell4@shutterfly.com', 'Female', '216.4.250.71');
insert into view_table1 (first_name, last_name, email, gender, ip_address) values ('Milzie', 'Rohlfing', 'mrohlfing5@java.com', 'Female', '230.101.87.42');
insert into view_table1 (first_name, last_name, email, gender, ip_address) values ('Gena', 'Varga', 'gvarga6@mapquest.com', 'Female', '170.240.242.112');
insert into view_table1 (first_name, last_name, email, gender, ip_address) values ('Guillermo', 'Hammill', 'ghammill7@nasa.gov', 'Male', '254.255.111.71');
insert into view_table1 (first_name, last_name, email, gender, ip_address) values ('Chelsey', 'Mably', 'cmably8@fc2.com', 'Female', '34.107.49.60');
insert into view_table1 (first_name, last_name, email, gender, ip_address) values ('Noak', 'Meecher', 'nmeecher9@quantcast.com', 'Male', '152.239.228.215');

drop table if exists view_table2;

create table view_table2 (
	id int primary key auto_increment,
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	email VARCHAR(50),
	gender VARCHAR(50),
	ip_address VARCHAR(20)
);
insert into view_table2 (first_name, last_name, email, gender, ip_address) values ('Aloysius', 'Capnerhurst', 'acapnerhurstz@goodreads.com', 'Male', '95.114.68.42');
insert into view_table2 (first_name, last_name, email, gender, ip_address) values ('Katusha', 'Jacob', 'kjacob10@answers.com', 'Female', '76.225.177.100');
insert into view_table2 (first_name, last_name, email, gender, ip_address) values ('Clywd', 'Rahl', 'crahl11@phoca.cz', 'Male', '108.153.62.82');
insert into view_table2 (first_name, last_name, email, gender, ip_address) values ('Darnell', 'Fyfield', 'dfyfield12@ucoz.com', 'Male', '246.157.90.10');
insert into view_table2 (first_name, last_name, email, gender, ip_address) values ('Myrlene', 'Connikie', 'mconnikie13@twitpic.com', 'Female', '54.208.146.115');
insert into view_table2 (first_name, last_name, email, gender, ip_address) values ('Ettore', 'Vossgen', 'evossgen14@com.com', 'Male', '156.26.89.33');
insert into view_table2 (first_name, last_name, email, gender, ip_address) values ('Christie', 'McGrory', 'cmcgrory15@ning.com', 'Female', '198.178.94.32');
insert into view_table2 (first_name, last_name, email, gender, ip_address) values ('Agatha', 'Amey', 'aamey16@hibu.com', 'Female', '132.36.221.179');
insert into view_table2 (first_name, last_name, email, gender, ip_address) values ('Ranee', 'Hast', 'rhast17@webeden.co.uk', 'Female', '68.206.219.63');


drop view if exists v1;

create view v1 as select first_name,last_name from view_table1 where gender="Female";

select * from v1;

drop view if exists v2;

create view v2 as select a.first_name,b.last_name from view_table1 a,view_table2 b where a.id=b.id;

select * from v2;

drop view if exists v3;

create view v3 as select a.first_name,b.last_name from view_table1 a inner join view_table2 b using(id);

select * from v3;

drop view if exists `whitespace view`;

create view `whitespace view` as select * from view_table1;

select * from `whitespace view`;

desc v1;
desc v2;
desc v3;
desc `whitespace view`;

