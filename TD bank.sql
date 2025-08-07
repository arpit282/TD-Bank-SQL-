-- Table 1 creation

create table Feed_1(

id serial Primary Key,

coll text, col2 text, col3 text, col4 text, col5 text, col6 text, col7 text, cols text, co19 text, col10 text
);

-- insertion of data into table

Insert into feed_1(col1, col2, col3, col4, col5, col6, col7,col8, col9,col10)
select
 random()*100, 
 random()*100, 
 random()*100, 
 random()*100, 
 random()*100, 
 random()*100,
 random()*100,
 random()*100, 
 random()*100, 
 random()*100 from generate_series(1,10);


select from feed 1;
----------------------------------------------------------------------------------------------------
# Table 2 creation

create table Feed_2(
id serial Primary Key,
coll text, col2 text, col3 text, col4 text, col5 text, col6 text, col7 text, col8 text, col9 text, col10 text, col11 text, col12 text, col13 text, col14 text, col15 text);

# Table 2

Insert into Feed_2 (coll, col2, col3, col4, col5, col6,col7,col8,col9,col10,col11, col12, col13,col14,col15)

select

random()*100, random()*100, random()*100,random()*100, random()*100, random()*100, random()*100, random()*100,

random()*100, random()*100,random()*100, random()*100, random()*100, random()*100, random()*100

from generate_series(1,15);

select from Feed_2;

----------------------------------------------------------------------------------------
-- Table 3

create table Feed_3(

id serial Primary Key,

coll text, col2 text, col3 text, col4 text, col5 text, col6 text, col7 text, col8 text, col9 text, col10 text, col11 text, col12 text, col13 text, col14 text, col15 text,
 col16 text, col17 text, col18 text, col19 text, col20 text);

Insert into Feed_3 (coll, col2, col3, col4, col5, col6,col7,col8,col9,col10,col11, col12, col13,col14,col15, col16, col17, col18, col19, col28)

select

random()*100, random()*100, random()*100,random()*100, random()*100, random()*100, random()*100, random()*100,

random()*100, random()*100,

random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100,

random()*100, random()*100

from generate_series(1,20);

select * from Feed_3;

-- Building the dynamic structure

create or replace PROCEDURE generate_feed(feed_name text, rows_no int)

LANGUAGE plpgsql

as $$

begin

execute format ('Drop table if exists I', feed_name);

execute format('

create table %I(

id serial PRIMARY KEY,
coll text, col2 text, col3 text, col4 text, col5 text, col6 text, col7 text, col8 text, col9 text, col10 text
)', feed_name);


EXECUTE format('

insert into %I (coll,col2, col3, col4,col5, col6, col7, cola, col9,col10)

select

random()*100, random()*100,random()*100,random()*100, random()*100, random()*100, random()*100,

random()*100, random()*100, random()*100

from generate_series(1,%s)

', feed_name, rows_no);

raise notice 'Successfully created the table: ',feed_name;

exception
when others THEN
raise notice "An error occured:%", SQLERRM;

end;
$$


call generate_feed('Feed_5',10);

select from "Feed_5"; 

call generate_feed('Feed_6',20);

select from "Feed_6";

# Finding Duplicates

# Table Duplicate 1
# Adding a duplicate value in the Feed 1 table

insert into feed_1 (coll,col2, col3,col4,col5,col6, col7, col8,co19,col10)

values ('90.0729517353527', '68.54429915999387', '86.12851461820911', '83.193229270372', '88.3621473456384', '23.34885328624454', '29.97480553255498', '36.62199665685637', '91.90531654002685');

select from feed_1;

-- Searching for the Duplicates in Feed 1

Drop table if exists duplicate_feedl;

create table duplicate_feedl as with duplicatel as (

select *, row_number() over(partition by coll,col2, col3, col4, col5, col6, col7, col8, col9, col10) as rn from feed_1 )

select * from duplicatel where rn 1;

select * from duplicate_feedl;

-- Table Duplicate 2

-- Adding a duplicate value in the Feed 1 table

insert into feed_2 (coll,col2,col3, col4, col5, col6, col7, col8, col9, col10, col11,col12, col13, col14, col15) values ('87.49301275484547', '81.3037998918972', '69.12776921160628', '93.91201170996378', '69.91805039824233', 3.245173485690378', '53.464343453395145', '17.83862676563448', '26.902811965062522 97.97556777587337,35.631193284556176', '5.916568842550363', '70.42580390057327'); 10.533476351929739", 11.139629847254318',

select * from feed_2;

drop table if exists duplicate_feed2;

create table duplicate_feed2 as with duplicate2 as ( select, row_number() over(partition by coli, col2, col3, col4,col5, col6, col7, col8, col9, col10, col11, col12, col13, col14,col15) as rn from feed 2 )

select

from duplicate2 where rn>1;

select from duplicate_feed2;

-- Table Duplicate 3

-- Adding a duplicate value in the Feed 1 table

insert into feed_3 (coll, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17,col18,col19,col20) 
values ('18.530744711611726', '64.41494834589707', '48.613139098117244', '96.20619899514422', '75.17875470546696', '28.18300102795177', '52.89088715092514', '54.689012086109415', '68.82635367076774', '17.70613726065209', 86.54147219495141', '56.58442623057275', '93.40138514212887', '32.50807762539889', '72.6783254241909, '0.262166500473171', 20.98415962933291' 82.46784672967429', '78.82711178813979','37.26146431889688');

select from feed_3;

drop table if exists duplicate_feed3;

create table duplicate_feed3 as

with duplicate3 as (

select, row_number() over(partition by coll,col2, col3, col4, col5, col6, col7, cola, col9,col10, col11, col12, col13,col14,col15,col16,col17,col18,col19,col20) as rn

from feed_3

)

select *

from duplicate3

where rn>1;

select from duplicate_feed3;

# Task 5 replace unique duplicate
-- Feed_1

with duplicatestodeletel as(

select id,

row_number() over(partition by coll, col2, col3, cold, col5, col6, col7, col8, col9, col10 order by id) as rn from feed_1 as rn
from feed_1)

delete from feed 1
where id in (select id from duplicatestodeletel where rn=1)


-- Feed_2

with duplicatestodelete2 as(

select id,

row_number() over(partition by coll,col2, col3, cold, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15 order by id) as rn from feed_2
)

delete from feed_2
where id in (select id from duplicatestodelete2 where rn>1);

-- Table 3

with duplicatestodelete3 as(
select id,
row_number() over(partition by coll,col2, col3, cold, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15,col16,col17,col18,col19, col20 order by id) as rn

from feed_3
)
delete from feed_3
where id in (select id from duplicatestodelete3 where rn>1);


-- Task 7
-- Compare Feed 2 with Feed 1

create or replace PROCEDURE compare(table_1 text, table_2 text)

LANGUAGE plpgsql

as $$

DECLARE

comp_table_name text:= format('comp_%s_%s',table_1, table_2);

BEGIN
execute format('drop table if exists %I',comp_table_name);
execute format('

create table %I as

select f2.*

from %I f1

join %I f2

using (coll,col2,col3, col4, col5, col6, col7, col8, col9, col10);'

,comp_table_name, table_1, table_2);

raise notice 'comparison successfully done';

exception

when others THEN

raise notice 'An error occured:%', SQLERRM;

end;
$$

call compare('feed_1', 'feed_2'); select from comp_feed_1_feed_2;

call compare('feed_1','feed_3'); select from comp_feed_1_feed_3;

-- Task 8
-- \copy comp_feed_1_feed_3 to '/server/path/to/your_table_data.csv' with (format csv,header);

insert into feed_2(coll,col2,col3, col4,col5,col6,co17,cola, col9,col10) values ('6.944443334411888', '81.3114109505785', '61.68980017291261', '73.94244816711893", 65.01019048848448', '36.74393713895832', '46.27038459113473', '57.72754227078258', '84.49679114717583', '6.025554891938152');

insert into feed_3(coll,col2, col3, col4,cols, colé,col7,cola, col9,col10) values ('6.944443334411088', '81.3114109505785', '61.68980017291261', '73.94244816711893', '65.01819048848448', '36.74393713895032', '46.27038459113473', '57.72754227878258', '84.49679114717583', '6.025554891930152');

select * from feed_3;

-- Compare Feed 3 with Feed 1

drop table if exists comp_feed3_feed1;
create table comp_feed3_feed1 AS
select f3.*
from feed_1 f1
join feed_3 f3
using (coll,col2, col3, col4,col5,col6,col7, col8,colo,col10);

select * from comp_feed3_feed1;

-- task 9 Automated in pervious queries
