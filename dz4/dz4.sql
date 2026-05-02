/* Занятие 12. Управление хранилищем данных // ДЗ
Домашнее задание:
1. Написать несколько транзакционных запросов с разными уровнями изоляции и проанализировать поведение системы при конкурентных операциях
2. Настроить параметры хранения для нескольких таблиц, настроить и протестировать внешние таблицы для загрузки данных.
*/

-- Выполнение.

-- 1. Написать несколько транзакционных запросов с разными уровнями изоляции и проанализировать поведение системы при конкурентных операциях

-- Создадим таблицу для тестирования поведения Greenplum при различных уровнях изоляции транзакций

CREATE TABLE ods.transaction_test(
  t_id INT
, t_val INT
)
WITH (appendonly = FALSE)
DISTRIBUTED BY (t_id);

-- Наполним таблицу данными для выполнения тестов

INSERT INTO ods.transaction_test (t_id,t_val) 
VALUES 
(1,100),
(2,200),
(3,300),
(4,400);

-- Проверим поведением Greenplum при использования уровня изоляции транзакций READ COMMITTED

-- Проверяем какой уровень изоляции установлен в данный момент 

SHOW transaction_isolation;

-- Первоначальное состояние данных в таблице ods.transaction_test https://disk.yandex.ru/i/rkqbNv_BFBio2g

-- Запустим транзакцию 

START TRANSACTION; 

UPDATE ods.transaction_test
SET t_val = 1000
WHERE t_id = 1;

SELECT * FROM ods.transaction_test

-- Если выполним код до этой точки, изменим данные но не зафиксируем изменения, то запрос выборки данных изнутри транзакции и снаружи вернёт разные данные https://disk.yandex.ru/i/RRBWdie8odV_6Q
-- Это поведение подтвреждает тот факт, что в режиме изоляции READ COMMITTED во время транзакции видны только зафиксированные изменения на момент начала транзакции и незафиксированные изменения в рамках своей транзакции.

COMMIT;

-- Проверим поведением Greenplum при использования уровня изоляции транзакций Serializable

-- Первоначальное состояние данных в таблице ods.transaction_test https://disk.yandex.ru/i/IWRLhnPh1ESsdA

-- Запустим транзакцию с уровнем изоляции транзакций Serializable

START TRANSACTION ISOLATION LEVEL SERIALIZABLE;

SELECT * FROM ods.transaction_test;

-- В этой точке выполним транзакцию по изменению данных с фиксацией изменений (запрос ниже). Если мы ещё раз выполним запрос SELECT, без заврешения транзакции, то увидим, что в рамках нашей транзакции данные не изменились, хотя другая транзакция уже выполнила фиксацию изменений https://disk.yandex.ru/i/18SwDxLVI762Sg
-- Это поведение подтверждает тот факт, что в режиме изоляции SERIALIZABLE во время выполнения транзакции видны данные зафиксированные на момент начала транзакци, даже если во время транзакции завершилась другая транзакция (данные изменения не будут видны в рамках текущей транзакции).
-- Другими словами, на данном уровне изоляции мы не видим зафиксированные изменения, произошедшие после начала выполняемой транзакции.

COMMIT;

START TRANSACTION ISOLATION LEVEL SERIALIZABLE;

UPDATE ods.transaction_test
SET t_val = 2000
WHERE t_id = 2;

SELECT * FROM ods.transaction_test

COMMIT;

-- 2. Настроить параметры хранения для нескольких таблиц, настроить и протестировать внешние таблицы для загрузки данных.

-- В отдельной базе данных Postgres существует несколько таблиц с данными, настроим их загрузку в Greenplum с помощью инструмента PXF.

-- Создадим внешние таблицы для загрузки данные из БД Postgres

CREATE EXTERNAL TABLE sources.customers_ext (
  customer_id int4
, c_name varchar(255)
, address varchar(255)
, credit_limit NUMERIC(10, 2)
)
LOCATION ('pxf://hr_poc.customers?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://**.***.**.***:6432/postgres&USER=*****&PASS=*****') ON
ALL
FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_import')
ENCODING 'UTF-8';

SELECT * FROM sources.customers_ext

CREATE EXTERNAL TABLE sources.employees_ext (
	  employee_id int4
	, first_name varchar(20)
	, last_name varchar(25)
	, email varchar(25)
	, phone_number varchar(20)
	, hire_date date
	, job_id varchar(10)
	, salary numeric(10, 2)
	, commission_pct numeric(4, 3)
	, manager_id int4
	, department_id int4
	, rating_e int4
)
LOCATION ('pxf://hr_poc.employees?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://**.***.**.***:6432/postgres&USER=*****&PASS=*****') ON
ALL
FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_import')
ENCODING 'UTF-8';

SELECT * FROM sources.employees_ext

CREATE EXTERNAL TABLE sources.orders_ext (
	  order_id int4
	, customer_id int4
	, status varchar(20)
	, salesman_id int4
	, order_date date
)
LOCATION ('pxf://hr_poc.orders?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://**.***.**.***:6432/postgres&USER=*****&PASS=*****') ON
ALL
FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_import')
ENCODING 'UTF-8';

SELECT * FROM sources.orders_ext

-- Создадим таблицы для хранения данных в Greenplum

CREATE TABLE IF NOT EXISTS sources.customers_src (
  customer_id int4 NOT NULL
, c_name varchar(255)
, address varchar(255)
, credit_limit NUMERIC(10,2)
)
WITH (
    APPENDONLY = TRUE,          
    COMPRESSTYPE = ZSTD,         
    COMPRESSLEVEL = 5,           
    ORIENTATION = COLUMN
)
DISTRIBUTED BY (customer_id);

CREATE TABLE IF NOT EXISTS sources.employees_src (
  employee_id int4 NOT NULL
, first_name varchar(20)
, last_name varchar(25)
, email varchar(25)
, phone_number varchar(20)
, hire_date date
, job_id varchar(10) 
, salary numeric(10, 2) 
, commission_pct numeric(4, 3) 
, manager_id int4 
, department_id int4 
, rating_e int4 
)
WITH (
    APPENDONLY = TRUE,          
    COMPRESSTYPE = ZSTD,         
    COMPRESSLEVEL = 5,           
    ORIENTATION = COLUMN
)
DISTRIBUTED BY (employee_id);

CREATE TABLE IF NOT EXISTS sources.orders_src (
  order_id int4 NOT NULL
, customer_id int4
, status varchar(20)
, salesman_id int4
, order_date date
)
WITH (
    APPENDONLY = TRUE,          
    COMPRESSTYPE = ZSTD,         
    COMPRESSLEVEL = 5,           
    ORIENTATION = COLUMN
)
DISTRIBUTED BY (order_id)
PARTITION BY RANGE (order_date)
(
    START ('2025-01-01') INCLUSIVE
    END ('2026-12-31') INCLUSIVE
    EVERY (INTERVAL '1 month'),
	DEFAULT PARTITION p_default
);

-- Вставим данные в таблицы из внешнних таблиц 

INSERT
	INTO
	sources.customers_src (
      customer_id
	, c_name
	, address
	, credit_limit
)
SELECT
	  customer_id
	, c_name
	, address
	, credit_limit
FROM
	sources.customers_ext;
	
	SELECT * FROM sources.customers_src;

INSERT
	INTO
	sources.employees_src (
      employee_id 
	, first_name 
	, last_name 
	, email 
	, phone_number 
	, hire_date
	, job_id 
	, salary 
	, commission_pct 
	, manager_id 
	, department_id 
	, rating_e 
)
SELECT
	   employee_id 
	 , first_name 
	 , last_name 
	 , email 
	 , phone_number 
	 , hire_date
	 , job_id 
	 , salary 
	 , commission_pct 
	 , manager_id 
	 , department_id 
	 , rating_e 
FROM
	sources.employees_ext;
	
	SELECT * FROM sources.employees_src;
	
INSERT
	INTO
	sources.orders_src (
      order_id 
	, customer_id 
	, status 
	, salesman_id 
	, order_date 
)
SELECT
	  order_id 
	, customer_id 
	, status 
	, salesman_id 
    , order_date 
FROM
	sources.orders_ext;
	
	SELECT * FROM sources.orders_src;
	
-- В результате мы успешно загрузили данные трёх таблиц Postgres в Greenplum.
