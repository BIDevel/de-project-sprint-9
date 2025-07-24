---------
-- cdm --
---------
create schema if not exists cdm;

drop table if exists cdm.user_product_counters cascade;
create table if not exists cdm.user_product_counters
(
 id int primary key generated always as identity not null,
 user_id uuid not null,
 product_id uuid not null ,
 product_name character varying not null,
 order_cnt int not null check(order_cnt >= 0)
);
create unique index if not exists uidx_cdm_user_product_counters_user_id_product_id on cdm.user_product_counters  using btree  (user_id, product_id) ;


drop table if exists cdm.user_category_counters cascade;
create table if not exists cdm.user_category_counters
(
 id int primary key generated always as identity not null,
 user_id uuid not null,
 category_id uuid not null ,
 category_name character varying not null,
 order_cnt int not null check(order_cnt >= 0)
);
create unique index if not exists uidx_cdm_user_category_counters_user_id_category_id on cdm.user_category_counters  using btree  (user_id, category_id) ;

---------
-- stg --
---------
create schema if not exists stg;

drop table if exists stg.order_events cascade;
create table if not exists stg.order_events
(
 id int primary key generated always as identity not null,
 object_id int unique not null,
 payload json not null, 
 object_type character varying not null,
 sent_dttm timestamp not null
);

---------------------
-- dds - DataVault --
---------------------
create schema if not exists dds;

-- hubs --
drop table if exists dds.h_user cascade;
create table if not exists dds.h_user
(
h_user_pk uuid not null primary key,
user_id character varying not null,
load_dt timestamp not null,
load_src character varying not null default 'orders-system-kafka'
);


drop table if exists dds.h_product cascade;
create table if not exists dds.h_product
(
h_product_pk uuid not null primary key,
product_id character varying not null,
load_dt timestamp not null,
load_src character varying not null default 'orders-system-kafka'
);


drop table if exists dds.h_category cascade;
create table if not exists dds.h_category
(
h_category_pk uuid not null primary key,
category_name character varying not null,
load_dt timestamp not null,
load_src character varying not null default 'orders-system-kafka'
);


drop table if exists dds.h_restaurant cascade;
create table if not exists dds.h_restaurant
(
h_restaurant_pk uuid not null primary key,
restaurant_id character varying not null,
load_dt timestamp not null,
load_src character varying not null default 'orders-system-kafka'
);


drop table if exists dds.h_order cascade;
create table if not exists dds.h_order
(
h_order_pk uuid not null primary key,
order_id int not null,
order_dt timestamp not null,
load_dt timestamp not null,
load_src character varying not null default 'orders-system-kafka'
);

-- links --

drop table if exists dds.l_order_product cascade;

create table if not exists dds.l_order_product
(
hk_order_product_pk uuid primary key not null,
h_order_pk uuid not null,
h_product_pk uuid not null,
load_dt timestamp default now() not null,
load_src varchar not null default 'orders-system-kafka'
);
alter table dds.l_order_product add constraint order_fk   foreign key(h_order_pk)   references dds.h_order(h_order_pk);
alter table dds.l_order_product add constraint product_fk foreign key(h_product_pk) references dds.h_product(h_product_pk);


drop table if exists dds.l_product_restaurant cascade;
create table if not exists dds.l_product_restaurant
(
hk_product_restaurant_pk uuid primary key not null,
h_restaurant_pk uuid not null,
h_product_pk uuid not null,
load_dt timestamp default now() not null,
load_src varchar not null  default 'orders-system-kafka'
);
alter table dds.l_product_restaurant add constraint restaurant_fk foreign key(h_restaurant_pk)   references dds.h_restaurant(h_restaurant_pk);
alter table dds.l_product_restaurant add constraint product_fk    foreign key(h_product_pk) references dds.h_product(h_product_pk);


drop table if exists dds.l_product_category cascade;
create table if not exists dds.l_product_category
(
hk_product_category_pk uuid primary key not null,
h_category_pk uuid not null,
h_product_pk uuid not null,
load_dt timestamp default now() not null,
load_src varchar not null default 'orders-system-kafka'
);
alter table dds.l_product_category add constraint category_fk foreign key(h_category_pk)  references dds.h_category(h_category_pk);
alter table dds.l_product_category add constraint product_fk    foreign key(h_product_pk) references dds.h_product(h_product_pk);


drop table if exists dds.l_order_user cascade;
create table if not exists dds.l_order_user
(
hk_order_user_pk uuid primary key not null,
h_user_pk uuid not null,
h_order_pk uuid not null,
load_dt timestamp default now() not null,
load_src varchar not null default 'orders-system-kafka'
);
alter table dds.l_order_user add constraint user_fk foreign key(h_user_pk)   references dds.h_user(h_user_pk);
alter table dds.l_order_user add constraint order_fk    foreign key(h_order_pk) references dds.h_order(h_order_pk);


-- satellits --

drop table if exists dds.s_user_names cascade;
create table if not exists dds.s_user_names (
h_user_pk uuid not null,
username varchar not null,
userlogin varchar not null,
load_dt timestamp default now() not null,
load_src varchar not null default 'orders-system-kafka',
hk_user_names_hashdiff uuid not null,
constraint PK_s_user_names primary key (h_user_pk, load_dt)
);
alter table dds.s_user_names add constraint s_user_names_fk foreign key( h_user_pk ) references dds.h_user(h_user_pk);


drop table if exists dds.s_product_names cascade;
create table if not exists dds.s_product_names (
h_product_pk uuid not null,
name varchar not null,
load_dt timestamp default now() not null,
load_src varchar not null default 'orders-system-kafka',
hk_product_names_hashdiff uuid unique not null,
constraint hk_product_names_pk primary key (h_product_pk, load_dt)
);
alter table dds.s_product_names add constraint s_product_names_fk foreign key( h_product_pk ) references dds.h_product(h_product_pk);


drop table if exists dds.s_restaurant_names cascade;
create table if not exists dds.s_restaurant_names (
h_restaurant_pk uuid not null,
name varchar not null,
load_dt timestamp default now() not null,
load_src varchar not null default 'orders-system-kafka',
hk_restaurant_names_hashdiff uuid unique not null,
constraint hk_restaurant_names_pk primary key (h_restaurant_pk, load_dt)
);
alter table dds.s_restaurant_names add constraint s_restaurant_names_fk foreign key( h_restaurant_pk ) references dds.h_restaurant(h_restaurant_pk);


drop table if exists dds.s_order_cost cascade;
create table if not exists dds.s_order_cost (
h_order_pk uuid not null,
cost decimal(19,5) not null,
payment decimal(19,5) not null,
load_dt timestamp default now() not null,
load_src varchar not null default 'orders-system-kafka',
hk_order_cost_hashdiff uuid not null,
constraint hk_order_cost_pk primary key (h_order_pk, load_dt)
);
alter table dds.s_order_cost add constraint s_order_cost_fk foreign key( h_order_pk ) references dds.h_order(h_order_pk);


drop table if exists dds.s_order_status cascade;
create table if not exists dds.s_order_status (
h_order_pk uuid not null,
status character varying not null,
load_dt timestamp default now() not null,
load_src varchar not null default 'orders-system-kafka',
hk_order_status_hashdiff uuid not null,
constraint hk_order_status_pk primary key (h_order_pk, load_dt)
);
alter table dds.s_order_status add constraint s_order_status_fk foreign key( h_order_pk ) references dds.h_order(h_order_pk);

