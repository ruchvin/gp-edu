/*Ход выолнения.

Распаковал архивы. Загрузил файлы на сервер.

В контейере докер создал папку для загружаемых через pfdist данных
docker exec -it <Container_name> /bin/bash
mkdir external_files

Загрузил файлы в контейнер Docker.
docker cp . greenplum-single-node:/home/gpdb/external_files

Удалил символы | в конце каждой строки во всех файла (если не удалить, возникает ошибка при обращении к внешней таблице)
sed -i 's/.$//' *

Запустил gpfdist в контейнере docker
docker exec -it <Container_name> /bin/bash
gpfdist -p 1111 -d /home/gpdb/external_files -l ../gpfdist.log &

*/

--Создал схему для загрузки данных, где будут хранится внешние таблицы
CREATE SCHEMA sources;

--Создал внешние таблицы для загрузки данных из файлов с помощью gpfdist

CREATE EXTERNAL TABLE sources.customer ( 
  C_CUSTKEY INT
, C_NAME text
, C_ADDRESS text
, C_NATIONKEY INTEGER
, C_PHONE CHAR(15)
, C_ACCTBAL DECIMAL(15, 2)
, C_MKTSEGMENT CHAR(10)
, C_COMMENT text) 
LOCATION 
('gpfdist://localhost:1111/customer.tbl-537401-4b92f8') 
FORMAT 'CSV' (DELIMITER '|');

SELECT * FROM sources.customer;

CREATE EXTERNAL TABLE sources.lineitem ( 
  L_ORDERKEY BIGINT
, L_PARTKEY INT
, L_SUPPKEY INT
, L_LINENUMBER INTEGER
, L_QUANTITY DECIMAL(15, 2)
, L_EXTENDEDPRICE DECIMAL(15, 2)
, L_DISCOUNT DECIMAL(15, 2)
, L_TAX DECIMAL(15, 2)
, L_RETURNFLAG CHAR(1)
, L_LINESTATUS CHAR(1)
, L_SHIPDATE DATE
, L_COMMITDATE DATE
, L_RECEIPTDATE DATE
, L_SHIPINSTRUCT CHAR(25)
, L_SHIPMODE CHAR(10)
, L_COMMENT text) 
LOCATION ('gpfdist://localhost:1111/lineitem.tbl-537401-7cbebc') 
FORMAT 'CSV' (DELIMITER '|');

SELECT * FROM sources.lineitem;

CREATE EXTERNAL TABLE sources.nation ( 
  N_NATIONKEY INTEGER
, N_NAME CHAR(25)
, N_REGIONKEY INTEGER
, N_COMMENT text) 
LOCATION ('gpfdist://localhost:1111/nation.tbl-537401-482dff') 
FORMAT 'CSV' (DELIMITER '|');

SELECT * FROM sources.region;

CREATE EXTERNAL TABLE sources.orders ( 
  O_ORDERKEY BIGINT
, O_CUSTKEY INT
, O_ORDERSTATUS CHAR(1)
, O_TOTALPRICE DECIMAL(15, 2)
, O_ORDERDATE DATE
, O_ORDERPRIORITY CHAR(15)
, O_CLERK CHAR(15)
, O_SHIPPRIORITY INTEGER
, O_COMMENT text) 
LOCATION ('gpfdist://localhost:1111/orders.tbl-537401-a0e313') 
FORMAT 'CSV' (DELIMITER '|');

SELECT * FROM sources.orders;

CREATE EXTERNAL TABLE sources.part ( 
  P_PARTKEY INT
, P_NAME text
, P_MFGR CHAR(25)
, P_BRAND CHAR(10)
, P_TYPE text
, P_SIZE INTEGER
, P_CONTAINER CHAR(10)
, P_RETAILPRICE DECIMAL(15, 2)
, P_COMMENT text) 
LOCATION ('gpfdist://localhost:1111/part.tbl-537401-807c1a') 
FORMAT 'CSV' (DELIMITER '|');

SELECT * FROM sources.part;

CREATE EXTERNAL TABLE sources.partsupp ( 
  PS_PARTKEY INT
, PS_SUPPKEY INT
, PS_AVAILQTY INTEGER
, PS_SUPPLYCOST DECIMAL(15, 2)
, PS_COMMENT text) 
LOCATION ('gpfdist://localhost:1111/partsupp.tbl-537401-8b2b85') 
FORMAT 'CSV' (DELIMITER '|');

SELECT * FROM sources.partsupp;

CREATE EXTERNAL TABLE sources.supplier ( 
  S_SUPPKEY INT
, S_NAME CHAR(25)
, S_ADDRESS text
, S_NATIONKEY INTEGER
, S_PHONE CHAR(15)
, S_ACCTBAL DECIMAL(15, 2)
, S_COMMENT text) 
LOCATION ('gpfdist://localhost:1111/supplier.tbl-537401-9303d6') 
FORMAT 'CSV' (DELIMITER '|');

SELECT * FROM sources.supplier;

CREATE EXTERNAL TABLE sources.region ( 
  R_REGIONKEY INTEGER
, R_NAME CHAR(25)
, R_COMMENT text) 
LOCATION ('gpfdist://localhost:1111/region.tbl-537401-52d5f8') 
FORMAT 'CSV' (DELIMITER '|');

SELECT * FROM sources.region;

--Создал схему для таблиц операционного слоя 
CREATE SCHEMA ods;

--Создал таблицы для загрузки данных из внешних таблиц

CREATE TABLE ods.customer (
    C_CUSTKEY INT,
    C_NAME text,
    C_ADDRESS text,
    C_NATIONKEY INTEGER,
    C_PHONE CHAR(15),
    C_ACCTBAL DECIMAL(15, 2),
    C_MKTSEGMENT CHAR(10),
    C_COMMENT text
) WITH (appendonly = true, orientation = column) 
DISTRIBUTED BY (C_CUSTKEY);

CREATE TABLE ods.lineitem (
    L_ORDERKEY BIGINT,
    L_PARTKEY INT,
    L_SUPPKEY INT,
    L_LINENUMBER INTEGER,
    L_QUANTITY DECIMAL(15, 2),
    L_EXTENDEDPRICE DECIMAL(15, 2),
    L_DISCOUNT DECIMAL(15, 2),
    L_TAX DECIMAL(15, 2),
    L_RETURNFLAG CHAR(1),
    L_LINESTATUS CHAR(1),
    L_SHIPDATE DATE,
    L_COMMITDATE DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT CHAR(25),
    L_SHIPMODE CHAR(10),
    L_COMMENT text
) WITH (
    appendonly = true,
    orientation = column,
    compresstype = ZSTD
) 
DISTRIBUTED BY (L_ORDERKEY, L_LINENUMBER) 
PARTITION BY RANGE (L_SHIPDATE) 
    (start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30), default partition others);

CREATE TABLE ods.nation (
    N_NATIONKEY INTEGER,
    N_NAME CHAR(25),
    N_REGIONKEY INTEGER,
    N_COMMENT text
) WITH (appendonly = true, orientation = column) 
DISTRIBUTED BY (N_NATIONKEY);

CREATE TABLE ods.orders (
    O_ORDERKEY BIGINT,
    O_CUSTKEY INT,
    O_ORDERSTATUS CHAR(1),
    O_TOTALPRICE DECIMAL(15, 2),
    O_ORDERDATE DATE,
    O_ORDERPRIORITY CHAR(15),
    O_CLERK CHAR(15),
    O_SHIPPRIORITY INTEGER,
    O_COMMENT text
) WITH (
    appendonly = true,
    orientation = column,
    compresstype = ZSTD
) 
DISTRIBUTED BY (O_ORDERKEY) 
PARTITION BY RANGE (O_ORDERDATE) 
    (start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30), default partition others);

CREATE TABLE ods.part (
    P_PARTKEY INT,
    P_NAME text,
    P_MFGR CHAR(25),
    P_BRAND CHAR(10),
    P_TYPE text,
    P_SIZE INTEGER,
    P_CONTAINER CHAR(10),
    P_RETAILPRICE DECIMAL(15, 2),
    P_COMMENT text
) WITH (appendonly = true, orientation = column) 
DISTRIBUTED BY (P_PARTKEY);

CREATE TABLE ods.partsupp (
    PS_PARTKEY INT,
    PS_SUPPKEY INT,
    PS_AVAILQTY INTEGER,
    PS_SUPPLYCOST DECIMAL(15, 2),
    PS_COMMENT text
) WITH (appendonly = true, orientation = column) 
DISTRIBUTED BY (PS_PARTKEY, PS_SUPPKEY);

CREATE TABLE ods.region (
    R_REGIONKEY INTEGER,
    R_NAME CHAR(25),
    R_COMMENT text
) WITH (appendonly = true, orientation = column) 
DISTRIBUTED BY (R_REGIONKEY);

CREATE TABLE ods.supplier (
    S_SUPPKEY INT,
    S_NAME CHAR(25),
    S_ADDRESS text,
    S_NATIONKEY INTEGER,
    S_PHONE CHAR(15),
    S_ACCTBAL DECIMAL(15, 2),
    S_COMMENT text
) WITH (appendonly = true, orientation = column) 
DISTRIBUTED BY (S_SUPPKEY);

--Вставил данных из внешних таблиц в таблицы слоя ods

INSERT
	INTO
	ods.customer (
	  C_CUSTKEY
	, C_NAME
	, C_ADDRESS
	, C_NATIONKEY
	, C_PHONE
	, C_ACCTBAL
	, C_MKTSEGMENT
	, C_COMMENT)
SELECT
	  C_CUSTKEY
	, C_NAME
	, C_ADDRESS
	, C_NATIONKEY
	, C_PHONE
	, C_ACCTBAL
	, C_MKTSEGMENT
	, C_COMMENT
FROM
	sources.customer;

SELECT * FROM ods.customer;

INSERT
	INTO
	ods.lineitem (
	  L_ORDERKEY
	, L_PARTKEY
	, L_SUPPKEY
	, L_LINENUMBER
	, L_QUANTITY
	, L_EXTENDEDPRICE
	, L_DISCOUNT
	, L_TAX
	, L_RETURNFLAG
	, L_LINESTATUS
	, L_SHIPDATE
	, L_COMMITDATE
	, L_RECEIPTDATE
	, L_SHIPINSTRUCT
	, L_SHIPMODE
	, L_COMMENT)
SELECT
	  L_ORDERKEY
	, L_PARTKEY
	, L_SUPPKEY
	, L_LINENUMBER
	, L_QUANTITY
	, L_EXTENDEDPRICE
	, L_DISCOUNT
	, L_TAX
	, L_RETURNFLAG
	, L_LINESTATUS
	, L_SHIPDATE
	, L_COMMITDATE
	, L_RECEIPTDATE
	, L_SHIPINSTRUCT
	, L_SHIPMODE
	, L_COMMENT
FROM
	sources.lineitem;

SELECT * FROM ods.lineitem;

INSERT
	INTO
	ods.nation (
	  N_NATIONKEY
	, N_NAME
	, N_REGIONKEY
	, N_COMMENT)
SELECT
	  N_NATIONKEY
	, N_NAME
	, N_REGIONKEY
	, N_COMMENT
FROM
	sources.nation;

SELECT * FROM ods.nation;

INSERT
	INTO
	ods.orders (
	  O_ORDERKEY
	, O_CUSTKEY
	, O_ORDERSTATUS
	, O_TOTALPRICE
	, O_ORDERDATE
	, O_ORDERPRIORITY
	, O_CLERK
	, O_SHIPPRIORITY
	, O_COMMENT)
SELECT
	  O_ORDERKEY
	, O_CUSTKEY
	, O_ORDERSTATUS
	, O_TOTALPRICE
	, O_ORDERDATE
	, O_ORDERPRIORITY
	, O_CLERK
	, O_SHIPPRIORITY
	, O_COMMENT
FROM
	sources.orders;

SELECT * FROM ods.orders;

INSERT
	INTO
	ods.part (
	  P_PARTKEY
	, P_NAME
	, P_MFGR
	, P_BRAND
	, P_TYPE
	, P_SIZE
	, P_CONTAINER
	, P_RETAILPRICE
	, P_COMMENT)
SELECT
	  P_PARTKEY
	, P_NAME
	, P_MFGR
	, P_BRAND
	, P_TYPE
	, P_SIZE
	, P_CONTAINER
	, P_RETAILPRICE
	, P_COMMENT
FROM
	sources.part;

SELECT * FROM ods.part;

INSERT
	INTO
	ods.partsupp (
	  PS_PARTKEY
	, PS_SUPPKEY
	, PS_AVAILQTY
	, PS_SUPPLYCOST
	, PS_COMMENT)
SELECT
	  PS_PARTKEY
	, PS_SUPPKEY
	, PS_AVAILQTY
	, PS_SUPPLYCOST
	, PS_COMMENT
FROM
	sources.partsupp;

SELECT * FROM ods.partsupp;

INSERT
	INTO
	ods.supplier (
	  S_SUPPKEY
	, S_NAME
	, S_ADDRESS
	, S_NATIONKEY
	, S_PHONE
	, S_ACCTBAL
	, s_COMMENT)
SELECT
	  S_SUPPKEY
	, S_NAME
	, S_ADDRESS
	, S_NATIONKEY
	, S_PHONE
	, S_ACCTBAL
	, S_COMMENT
FROM
	sources.supplier;

SELECT * FROM ods.supplier;

INSERT
	INTO
	ods.region (
	  R_REGIONKEY
	, R_NAME
	, R_COMMENT)
SELECT
	  R_REGIONKEY
	, R_NAME
	, R_COMMENT
FROM
	sources.region;

SELECT * FROM ods.region;

--Создал анонимную функцию для измерения времени выполнения запроса

DO $$

DECLARE

lv_start_time TIMESTAMP;
lv_end_time TIMESTAMP;
lv_query_duration NUMERIC;
lv_query_result RECORD;

BEGIN

lv_start_time := clock_timestamp();

RAISE NOTICE 'START QUERY  | %', lv_start_time;

SELECT * INTO lv_query_result FROM ods.customer c
JOIN ods.orders o ON c.c_custkey = o.o_custkey
JOIN ods.lineitem l ON o.o_orderkey = l.l_orderkey
JOIN ods.part p ON l.l_partkey = p.p_partkey
JOIN ods.partsupp ps ON p.p_partkey = ps.ps_partkey
JOIN ods.supplier s ON ps.ps_suppkey = s.s_suppkey
JOIN ods.nation n ON s.s_nationkey = n.n_nationkey
JOIN ods.region r ON n.n_regionkey = r.r_regionkey
WHERE c.c_mktsegment = 'BUILDING'
AND o.o_orderpriority = '3-MEDIUM'
AND l.l_shipmode = 'SHIP'
AND (p.p_container = 'LG BAG' OR p.p_container = 'LG CASE')
AND (r.r_name = 'AMERICA' OR r.r_name = 'EUROPE')
AND n.n_name IN ('UNITED STATES', 'GERMANY', 'INDIA', 'KENYA');

lv_end_time := clock_timestamp();

RAISE NOTICE 'FINISH QUERY | % ', lv_end_time;

lv_query_duration := EXTRACT(EPOCH FROM (lv_end_time - lv_start_time));

RAISE NOTICE 'DURATION QUERY % SECONDS', ROUND(lv_query_duration,2);

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Ошибка: %', SQLERRM;

END
$$;

--Результаты вызова функции ДО добавления партиций по списку (большинство результатов > 1 секунды)
START QUERY  | 2026-04-21 19:58:51.560846
FINISH QUERY | 2026-04-21 19:58:52.899194
DURATION QUERY 1.34 SECONDS

--Добавил партиционирование по списку (по периоду уже было настроено) к таблицам

DROP TABLE IF EXISTS ods.customer;

CREATE TABLE ods.customer (
    C_CUSTKEY INT,
    C_NAME text,
    C_ADDRESS text,
    C_NATIONKEY INTEGER,
    C_PHONE CHAR(15),
    C_ACCTBAL DECIMAL(15, 2),
    C_MKTSEGMENT CHAR(10),
    C_COMMENT text
) WITH (appendonly = true, orientation = column) 
DISTRIBUTED BY (C_CUSTKEY)
PARTITION BY LIST (C_MKTSEGMENT) (
PARTITION BUILDING VALUES ('BUILDING'),
PARTITION HOUSEHOLD VALUES ('HOUSEHOLD'),
PARTITION FURNITURE VALUES ('FURNITURE'),
PARTITION AUTOMOBILE VALUES ('AUTOMOBILE'),
PARTITION MACHINERY VALUES ('MACHINERY'),
DEFAULT PARTITION OTHER_SEGMENTS
);      

INSERT
	INTO
	ods.customer (
	  C_CUSTKEY
	, C_NAME
	, C_ADDRESS
	, C_NATIONKEY
	, C_PHONE
	, C_ACCTBAL
	, C_MKTSEGMENT
	, C_COMMENT)
SELECT
	  C_CUSTKEY
	, C_NAME
	, C_ADDRESS
	, C_NATIONKEY
	, C_PHONE
	, C_ACCTBAL
	, C_MKTSEGMENT
	, C_COMMENT
FROM
	sources.customer;
	
	
DROP TABLE IF EXISTS ods.orders;

CREATE TABLE ods.orders (
    O_ORDERKEY BIGINT,
    O_CUSTKEY INT,
    O_ORDERSTATUS CHAR(1),
    O_TOTALPRICE DECIMAL(15, 2),
    O_ORDERDATE DATE,
    O_ORDERPRIORITY CHAR(15),
    O_CLERK CHAR(15),
    O_SHIPPRIORITY INTEGER,
    O_COMMENT text
) WITH (
    appendonly = true,
    orientation = column,
    compresstype = ZSTD
) 
DISTRIBUTED BY (O_ORDERKEY) 
PARTITION BY RANGE (O_ORDERDATE)
SUBPARTITION BY LIST (O_ORDERPRIORITY)
SUBPARTITION TEMPLATE ( 
    SUBPARTITION urgent VALUES ('1-URGENT'),  
    SUBPARTITION high VALUES ('2-HIGH'), 
    SUBPARTITION medium VALUES ('3-MEDIUM'), 
    SUBPARTITION not_specified VALUES ('4-NOT SPECIFIED'),  
    SUBPARTITION low VALUES ('5-LOW'), 
    DEFAULT SUBPARTITION other_priority
)
(
start('1992-01-01') INCLUSIVE 
end ('1998-12-31') INCLUSIVE every (30), 
default partition others
);

INSERT
	INTO
	ods.orders (
	  O_ORDERKEY
	, O_CUSTKEY
	, O_ORDERSTATUS
	, O_TOTALPRICE
	, O_ORDERDATE
	, O_ORDERPRIORITY
	, O_CLERK
	, O_SHIPPRIORITY
	, O_COMMENT)
SELECT
	  O_ORDERKEY
	, O_CUSTKEY
	, O_ORDERSTATUS
	, O_TOTALPRICE
	, O_ORDERDATE
	, O_ORDERPRIORITY
	, O_CLERK
	, O_SHIPPRIORITY
	, O_COMMENT
FROM
	sources.orders;
	
--Результаты вызова функции ПОСЛЕ добавления партиций по списку (большинство результатов < 1 секунды)
START QUERY  | 2026-04-21 20:35:38.337839
FINISH QUERY | 2026-04-21 20:35:39.195086
DURATION QUERY 0.86 SECONDS

--Вывод: добавление партиционирования по списку позволило ускорить выполнение запроса, вероятно за счет того, что планироовщик сразу обращается к данным в нужной партиции, а не сканирует все данные на сегментах.