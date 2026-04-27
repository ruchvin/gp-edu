/*
Занятие 9. Индексы и их использование // ДЗ

Инструкция:

Используя датасет из ДЗ в занятии «SQL в Greenlum», напишите следующие запросы:
Query 1: Retrieve Customer Orders with Order and Customer Details
Query 2: Retrieve Detailed Order Information with Line Items
Query 3: Retrieve Supplier and Part Information for Each Supplier-Part Relationship
Query 4: Retrieve Comprehensive Customer Order and Line Item Details
Query 5: Retrieve All Parts Supplied by a Specific Supplier with Supplier Details

Замерьте время выполнения, сохраните планы запросов.

Используя материалы вебинара, оптимизируйте планы запросов. Примените vacuum, analyze, партиционирования и построение индексов.

Сравните время выполнения запросов с индексами и без, соберите информацию по использованию индексов из системных таблиц и представлений.
Сделайте вывод о необходимости индексов в Greenplum.

*/

-- 1. Используя датасет из ДЗ в занятии «SQL в Greenlum», напишите следующие запросы

-- Query 1: Retrieve Customer Orders with Order and Customer Details

SELECT
	  o.o_orderkey
	, o.o_orderstatus
	, o.o_totalprice
	, o.o_orderdate
	, o.o_orderpriority
	, o.o_clerk
	, o.o_shippriority
	, o.o_comment
	, c.c_name
	, c.c_custkey
	, c.c_address
	, n.n_name
	, c.c_phone
	, c.c_acctbal
	, c.c_mktsegment
FROM
	ods.orders o
JOIN ods.customer c ON
	o.o_custkey = c.c_custkey
JOIN ods.nation n ON
	c.c_nationkey = n.n_nationkey;

-- Query 2: Retrieve Detailed Order Information with Line Items

SELECT
	  o.o_orderkey
	, o.o_orderstatus
	, o.o_totalprice
	, o.o_orderdate
	, o.o_orderpriority
	, o.o_clerk
	, o.o_shippriority
	, o.o_comment
	, l.l_linenumber
	, l.l_quantity
	, l.l_extendedprice
	, l.l_discount
	, l.l_tax
	, l.l_returnflag
	, l.l_linestatus
	, l.l_shipdate
	, l.l_commitdate
	, l.l_receiptdate
	, l.l_shipinstruct
	, l.l_shipmode
	, l.l_comment
FROM
	ods.orders o
JOIN ods.lineitem l ON
	o.o_orderkey = l.l_orderkey;

-- Query 3: Retrieve Supplier and Part Information for Each Supplier-Part Relationship

SELECT
	  s.s_suppkey 		
	, s.s_name 			
	, s.s_address 		
	, n.n_name
	, s.s_phone 		
	, s.s_acctbal 		
	, s.s_comment 		
	, p.ps_availqty 	
	, p.ps_supplycost 	
	, p.ps_comment 		
	, pa.p_partkey 		
	, pa.p_name 		
	, pa.p_mfgr 		
	, pa.p_brand 		
	, pa.p_type 		
	, pa.p_size 		
	, pa.p_container 	
	, pa.p_retailprice 	
	, pa.p_comment 		
FROM
	ods.supplier s
JOIN ods.partsupp p ON
	s.s_suppkey  = p.ps_suppkey 
JOIN ods.part pa ON 
 p.ps_partkey = pa.p_partkey 
JOIN ods.nation n ON 
s.s_nationkey = n.n_nationkey;

-- Query 4: Retrieve Comprehensive Customer Order and Line Item Details

SELECT
	  c.c_custkey 
	, c.c_name 
	, n.n_name 
	, c.c_address 
	, o.o_orderkey
	, o.o_orderstatus
	, o.o_totalprice
	, o.o_orderdate
	, o.o_orderpriority
	, o.o_clerk
	, o.o_shippriority
	, o.o_comment
	, l.l_linenumber
	, l.l_quantity
	, l.l_extendedprice
	, l.l_discount
	, l.l_tax
	, l.l_returnflag
	, l.l_linestatus
	, l.l_shipdate
	, l.l_commitdate
	, l.l_receiptdate
	, l.l_shipinstruct
	, l.l_shipmode
	, l.l_comment
FROM
	ods.orders o
JOIN ods.lineitem l ON
	o.o_orderkey = l.l_orderkey
JOIN ods.customer c ON 
	o.o_custkey = c.c_custkey 
JOIN ods.nation n ON 
c.c_nationkey = n.n_nationkey
WHERE c.c_custkey  = 22;

-- Query 5: Retrieve All Parts Supplied by a Specific Supplier with Supplier Details

SELECT
	  s.s_suppkey 		
	, s.s_name 			
	, s.s_address 		
	, n.n_name
	, s.s_phone 		
	, pa.p_partkey 		
	, pa.p_name 		
	, pa.p_mfgr 		
	, pa.p_brand 		
	, pa.p_type 		
	, pa.p_size 		
	, pa.p_container 	
	, pa.p_retailprice 	
	, pa.p_comment 		
FROM
	ods.supplier s
JOIN ods.partsupp p ON
	s.s_suppkey  = p.ps_suppkey 
JOIN ods.part pa ON 
 p.ps_partkey = pa.p_partkey 
JOIN ods.nation n ON 
s.s_nationkey = n.n_nationkey
WHERE s.s_suppkey = 2;

-- 2. Замерьте время выполнения, сохраните планы запросов.

-- Время выполнения запросов измерял с помощью анонимной функции

DO $$

DECLARE

lv_start_time TIMESTAMP;
lv_end_time TIMESTAMP;
lv_query_duration NUMERIC;
lv_query_result RECORD;

BEGIN

lv_start_time := clock_timestamp();

RAISE NOTICE 'START QUERY  | %', lv_start_time;

-- Запрос для измерения

lv_end_time := clock_timestamp();

RAISE NOTICE 'FINISH QUERY | % ', lv_end_time;

lv_query_duration := EXTRACT(EPOCH FROM (lv_end_time - lv_start_time));

RAISE NOTICE 'DURATION QUERY % SECONDS', ROUND(lv_query_duration,2);

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Ошибка: %', SQLERRM;
END
$$;

-- Первоначальные результаты и планы запросов 

-- Query 1: Retrieve Customer Orders with Order and Customer Details
START QUERY  | 2026-04-25 21:57:51.58269
FINISH QUERY | 2026-04-25 21:57:52.097249
DURATION QUERY 0.51 SECONDS	
-- Чаще всего > 0.5 секнуд.

                          Hash Key: customer.c_nationkey
                          ->  Sequence  (cost=0.00..431.00 rows=1 width=57) (actual time=0.397..6.348 rows=30000 loops=1)
                                ->  Partition Selector for customer (dynamic scan id: 2)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                                      Partitions selected: 6 (out of 6)
                                ->  Dynamic Seq Scan on customer (dynamic scan id: 2)  (cost=0.00..431.00 rows=1 width=57) (actual time=0.391..5.013 rows=30000 loops=1)
                                      Partitions scanned:  6 (out of 6) .
  ->  Hash  (cost=431.00..431.00 rows=1 width=67) (actual time=489.734..489.734 rows=300000 loops=1)
        Buckets: 131072  Batches: 1  Memory Usage: 43900kB
        ->  Gather Motion 1:1  (slice3; segments: 1)  (cost=0.00..431.00 rows=1 width=67) (actual time=2.248..458.946 rows=300000 loops=1)
              ->  Sequence  (cost=0.00..431.00 rows=1 width=67) (actual time=0.782..349.750 rows=300000 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                          Partitions selected: 522 (out of 522)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..431.00 rows=1 width=67) (actual time=0.748..280.649 rows=300000 loops=1)
                          Partitions scanned:  522 (out of 522) .
Planning time: 32.378 ms
  (slice0)    Executor memory: 75301K bytes.  Work_mem: 43900K bytes max.
  (slice1)    Executor memory: 4474K bytes (seg0).
  (slice2)    Executor memory: 9544K bytes (seg0).  Work_mem: 3534K bytes max.
  (slice3)    Executor memory: 514717K bytes (seg0).
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 593.582 ms

-- Query 2: Retrieve Detailed Order Information with Line Items
START QUERY  | 2026-04-25 22:00:44.060055
FINISH QUERY | 2026-04-25 22:00:44.665221
DURATION QUERY 0.61 SECONDS
-- Чаще всего > 0.6 секнуд.

  Extra Text: Hash chain length 1.7 avg, 9 max, using 178828 of 262144 buckets.
  ->  Gather Motion 1:1  (slice1; segments: 1)  (cost=0.00..431.00 rows=1 width=101) (actual time=0.007..263.185 rows=1199969 loops=1)
        ->  Sequence  (cost=0.00..431.00 rows=1 width=101) (actual time=0.697..335.725 rows=1199969 loops=1)
              ->  Partition Selector for lineitem (dynamic scan id: 2)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                    Partitions selected: 87 (out of 87)
              ->  Dynamic Seq Scan on lineitem (dynamic scan id: 2)  (cost=0.00..431.00 rows=1 width=101) (actual time=0.692..295.116 rows=1199969 loops=1)
                    Partitions scanned:  87 (out of 87) .
  ->  Hash  (cost=431.00..431.00 rows=1 width=63) (actual time=564.734..564.734 rows=300000 loops=1)
        Buckets: 262144  Batches: 1  Memory Usage: 42808kB
        ->  Gather Motion 1:1  (slice2; segments: 1)  (cost=0.00..431.00 rows=1 width=63) (actual time=0.690..517.292 rows=300000 loops=1)
              ->  Sequence  (cost=0.00..431.00 rows=1 width=63) (actual time=0.361..349.499 rows=300000 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                          Partitions selected: 522 (out of 522)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..431.00 rows=1 width=63) (actual time=0.339..279.001 rows=300000 loops=1)
                          Partitions scanned:  522 (out of 522) .
Planning time: 8.901 ms
  (slice0)    Executor memory: 76117K bytes.  Work_mem: 42808K bytes max.
  (slice1)    Executor memory: 163138K bytes (seg0).
  (slice2)    Executor memory: 464577K bytes (seg0).
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 1276.313 ms

-- Query 3: Retrieve Supplier and Part Information for Each Supplier-Part Relationship
START QUERY  | 2026-04-25 22:02:45.548043
FINISH QUERY | 2026-04-25 22:02:45.601393
DURATION QUERY 0.05 SECONDS
-- Чаще всего > 0.05 секнуд.

Hash Join  (cost=0.00..2606.14 rows=160000 width=432) (actual time=20.587..90.664 rows=160000 loops=1)
  Hash Cond: (partsupp.ps_suppkey = supplier.s_suppkey)
  Extra Text: Hash chain length 1.0 avg, 2 max, using 1944 of 32768 buckets.
  ->  Hash Join  (cost=0.00..1436.02 rows=160000 width=269) (actual time=17.434..64.962 rows=160000 loops=1)
        Hash Cond: (partsupp.ps_partkey = part.p_partkey)
        Extra Text: Hash chain length 1.7 avg, 8 max, using 23026 of 32768 buckets.
        ->  Gather Motion 1:1  (slice1; segments: 1)  (cost=0.00..642.50 rows=160000 width=143) (actual time=0.004..14.643 rows=160000 loops=1)
              ->  Seq Scan on partsupp  (cost=0.00..444.82 rows=160000 width=143) (actual time=0.185..15.273 rows=160000 loops=1)
        ->  Hash  (cost=479.10..479.10 rows=40000 width=130) (actual time=17.415..17.415 rows=40000 loops=1)
              Buckets: 32768  Batches: 1  Memory Usage: 6654kB
              ->  Gather Motion 1:1  (slice2; segments: 1)  (cost=0.00..479.10 rows=40000 width=130) (actual time=0.004..15.417 rows=40000 loops=1)
                    ->  Seq Scan on part  (cost=0.00..434.17 rows=40000 width=130) (actual time=0.274..4.495 rows=40000 loops=1)
  ->  Hash  (cost=866.40..866.40 rows=2000 width=167) (actual time=3.104..3.104 rows=2000 loops=1)
        Buckets: 32768  Batches: 1  Memory Usage: 399kB
        ->  Hash Join  (cost=0.00..866.40 rows=2000 width=167) (actual time=1.792..2.532 rows=2000 loops=1)
              Hash Cond: (supplier.s_nationkey = nation.n_nationkey)
              Extra Text: Hash chain length 1.0 avg, 1 max, using 25 of 131072 buckets.
              ->  Gather Motion 1:1  (slice3; segments: 1)  (cost=0.00..433.68 rows=2000 width=145) (actual time=1.097..1.427 rows=2000 loops=1)
                    ->  Seq Scan on supplier  (cost=0.00..431.17 rows=2000 width=145) (actual time=0.126..0.314 rows=2000 loops=1)
              ->  Hash  (cost=431.01..431.01 rows=25 width=30) (actual time=0.570..0.570 rows=25 loops=1)
                    Buckets: 131072  Batches: 1  Memory Usage: 2kB
                    ->  Gather Motion 1:1  (slice4; segments: 1)  (cost=0.00..431.01 rows=25 width=30) (actual time=0.562..0.564 rows=25 loops=1)
                          ->  Seq Scan on nation  (cost=0.00..431.00 rows=25 width=30) (actual time=0.014..0.016 rows=25 loops=1)
Planning time: 32.784 ms
  (slice0)    Executor memory: 21554K bytes.  Work_mem: 6654K bytes max.
  (slice1)    Executor memory: 608K bytes (seg0).
  (slice2)    Executor memory: 992K bytes (seg0).
  (slice3)    Executor memory: 784K bytes (seg0).
  (slice4)    Executor memory: 268K bytes (seg0).
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 102.850 ms

-- Query 4: Retrieve Comprehensive Customer Order and Line Item Details
START QUERY  | 2026-04-25 22:04:11.220709
FINISH QUERY | 2026-04-25 22:04:11.399437
DURATION QUERY 0.18 SECONDS
-- Чаще всего > 0.18 секнуд.

Hash Join  (cost=0.00..1724.02 rows=1 width=202) (actual time=258.249..1218.940 rows=45 loops=1)
  Hash Cond: (orders.o_custkey = customer.c_custkey)
  Extra Text: Hash chain length 1.0 avg, 1 max, using 1 of 65536 buckets.
  ->  Hash Join  (cost=0.00..862.00 rows=1 width=160) (actual time=254.233..1214.832 rows=45 loops=1)
        Hash Cond: (lineitem.l_orderkey = orders.o_orderkey)
        Extra Text: Hash chain length 1.0 avg, 1 max, using 13 of 65536 buckets.
        ->  Gather Motion 1:1  (slice1; segments: 1)  (cost=0.00..431.00 rows=1 width=101) (actual time=0.006..912.655 rows=1199969 loops=1)
              ->  Sequence  (cost=0.00..431.00 rows=1 width=101) (actual time=1.412..551.320 rows=1199969 loops=1)
                    ->  Partition Selector for lineitem (dynamic scan id: 2)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Seq Scan on lineitem (dynamic scan id: 2)  (cost=0.00..431.00 rows=1 width=101) (actual time=1.398..489.953 rows=1199969 loops=1)
                          Partitions scanned:  87 (out of 87) .
        ->  Hash  (cost=431.00..431.00 rows=1 width=67) (actual time=246.374..246.374 rows=13 loops=1)
              Buckets: 65536  Batches: 1  Memory Usage: 2kB
              ->  Gather Motion 1:1  (slice2; segments: 1)  (cost=0.00..431.00 rows=1 width=67) (actual time=246.367..246.369 rows=13 loops=1)
                    ->  Sequence  (cost=0.00..431.00 rows=1 width=67) (actual time=9.775..249.073 rows=13 loops=1)
                          ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                                Partitions selected: 522 (out of 522)
                          ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..431.00 rows=1 width=67) (actual time=9.752..249.049 rows=13 loops=1)
                                Filter: (o_custkey = 22)
                                Partitions scanned:  522 (out of 522) .
  ->  Hash  (cost=862.01..862.01 rows=1 width=46) (actual time=3.969..3.969 rows=1 loops=1)
        Buckets: 65536  Batches: 1  Memory Usage: 1kB
        ->  Gather Motion 1:1  (slice4; segments: 1)  (cost=0.00..862.01 rows=1 width=46) (actual time=3.966..3.966 rows=1 loops=1)
              ->  Hash Join  (cost=0.00..862.01 rows=1 width=46) (actual time=3.434..3.519 rows=1 loops=1)
                    Hash Cond: (nation.n_nationkey = customer.c_nationkey)
                    Extra Text: Hash chain length 1.0 avg, 1 max, using 1 of 131072 buckets.
                    ->  Seq Scan on nation  (cost=0.00..431.00 rows=25 width=30) (actual time=0.024..0.027 rows=25 loops=1)
                    ->  Hash  (cost=431.00..431.00 rows=1 width=24) (actual time=3.086..3.086 rows=1 loops=1)
                          ->  Redistribute Motion 1:1  (slice3; segments: 1)  (cost=0.00..431.00 rows=1 width=24) (actual time=3.084..3.085 rows=1 loops=1)
                                Hash Key: customer.c_nationkey
                                ->  Sequence  (cost=0.00..431.00 rows=1 width=24) (actual time=0.322..2.566 rows=1 loops=1)
                                      ->  Partition Selector for customer (dynamic scan id: 3)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                                            Partitions selected: 6 (out of 6)
                                      ->  Dynamic Seq Scan on customer (dynamic scan id: 3)  (cost=0.00..431.00 rows=1 width=24) (actual time=0.318..2.562 rows=1 loops=1)
                                            Filter: (c_custkey = 22)
                                            Partitions scanned:  6 (out of 6) .
Planning time: 34.591 ms
  (slice0)    Executor memory: 1619K bytes.  Work_mem: 2K bytes max.
  (slice1)    Executor memory: 163140K bytes (seg0).
  (slice2)    Executor memory: 514701K bytes (seg0).
  (slice3)    Executor memory: 2729K bytes (seg0).
  (slice4)    Executor memory: 1344K bytes (seg0).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 1223.235 ms

-- Query 5: Retrieve All Parts Supplied by a Specific Supplier with Supplier Details
START QUERY  | 2026-04-25 22:06:49.76409
FINISH QUERY | 2026-04-25 22:06:49.812921
DURATION QUERY 0.05 SECONDS
-- Чаще всего > 0.05 секнуд.

Hash Join  (cost=0.00..1766.50 rows=75 width=228) (actual time=10.970..13.585 rows=80 loops=1)
  Hash Cond: (partsupp.ps_suppkey = supplier.s_suppkey)
  Extra Text: Hash chain length 1.0 avg, 1 max, using 1 of 65536 buckets.
  ->  Gather Motion 1:1  (slice2; segments: 1)  (cost=0.00..904.16 rows=75 width=134) (actual time=8.489..11.040 rows=80 loops=1)
        ->  Hash Join  (cost=0.00..904.10 rows=75 width=134) (actual time=6.207..12.832 rows=80 loops=1)
              Hash Cond: (part.p_partkey = partsupp.ps_partkey)
              Extra Text: Hash chain length 1.0 avg, 1 max, using 80 of 262144 buckets.
              ->  Seq Scan on part  (cost=0.00..434.17 rows=40000 width=130) (actual time=0.305..4.120 rows=40000 loops=1)
              ->  Hash  (cost=450.08..450.08 rows=75 width=8) (actual time=5.345..5.345 rows=80 loops=1)
                    ->  Redistribute Motion 1:1  (slice1; segments: 1)  (cost=0.00..450.08 rows=75 width=8) (actual time=5.333..5.336 rows=80 loops=1)
                          Hash Key: partsupp.ps_partkey
                          ->  Seq Scan on partsupp  (cost=0.00..450.08 rows=75 width=8) (actual time=0.100..4.581 rows=80 loops=1)
                                Filter: (ps_suppkey = 2)
  ->  Hash  (cost=862.25..862.25 rows=1 width=98) (actual time=2.403..2.403 rows=1 loops=1)
        Buckets: 65536  Batches: 1  Memory Usage: 1kB
        ->  Gather Motion 1:1  (slice4; segments: 1)  (cost=0.00..862.25 rows=1 width=98) (actual time=2.400..2.400 rows=1 loops=1)
              ->  Hash Join  (cost=0.00..862.25 rows=1 width=98) (actual time=0.565..0.624 rows=1 loops=1)
                    Hash Cond: (nation.n_nationkey = supplier.s_nationkey)
                    Extra Text: Hash chain length 1.0 avg, 1 max, using 1 of 65536 buckets.
                    ->  Seq Scan on nation  (cost=0.00..431.00 rows=25 width=30) (actual time=0.038..0.043 rows=25 loops=1)
                    ->  Hash  (cost=431.24..431.24 rows=1 width=76) (actual time=0.227..0.227 rows=1 loops=1)
                          ->  Redistribute Motion 1:1  (slice3; segments: 1)  (cost=0.00..431.24 rows=1 width=76) (actual time=0.225..0.225 rows=1 loops=1)
                                Hash Key: supplier.s_nationkey
                                ->  Seq Scan on supplier  (cost=0.00..431.24 rows=1 width=76) (actual time=0.139..0.291 rows=1 loops=1)
                                      Filter: (s_suppkey = 2)
Planning time: 22.680 ms
  (slice0)    Executor memory: 2609K bytes.  Work_mem: 1K bytes max.
  (slice1)    Executor memory: 268K bytes (seg0).
  (slice2)    Executor memory: 3128K bytes (seg0).  Work_mem: 3K bytes max.
  (slice3)    Executor memory: 592K bytes (seg0).
  (slice4)    Executor memory: 832K bytes (seg0).  Work_mem: 1K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 21.450 ms

-- 3. Используя материалы вебинара, оптимизируйте планы запросов. Примените vacuum, analyze, партиционирования и построение индексов.

-- Выполним операции по сбору статистики для всех таблиц (они не были проведены после загрузки данных), с целью помочь планирощику составлять более адекватные планы запросов.

ANALYZE ods.customer;

ANALYZE ods.lineitem;

ANALYZE ods.nation;

ANALYZE ods.orders;

ANALYZE ods.part;

ANALYZE ods.partsupp;

ANALYZE ods.region;

ANALYZE ods.supplier;		

-- Создадаим индексы (B-Tree) для полей по которым происходит объединение и фильтрация 
-- Выбрал тип B-Tree (создается по умолчанию) так как на мой взгляд он лучше всего подходит для целочесленных полей с высокой уникальность, как например: номер заказа, номер клиента и тд.

CREATE INDEX o_orderkey_idx ON ods.orders (o_orderkey);

CREATE INDEX o_custkey_idx ON ods.orders (o_custkey);

ANALYZE ods.orders;

CREATE INDEX c_custkey_idx ON ods.customer (c_custkey);

CREATE INDEX c_nationkey_idx ON ods.customer (c_nationkey);

ANALYZE ods.customer;

CREATE INDEX n_nationkey_idx ON ods.nation (n_nationkey);

ANALYZE ods.nation;

CREATE INDEX l_orderkey_idx ON ods.lineitem (l_orderkey);

CREATE INDEX l_partkey_idx ON ods.lineitem (l_partkey);

CREATE INDEX l_suppkey_idx ON ods.lineitem (l_suppkey);

ANALYZE ods.lineitem;

CREATE INDEX p_partkey_idx ON ods.part (p_partkey);

ANALYZE ods.part;

CREATE INDEX ps_partkey_idx ON ods.partsupp (ps_partkey);

CREATE INDEX ps_suppkey_idx ON ods.partsupp (ps_suppkey);

ANALYZE ods.partsupp;

CREATE INDEX s_suppkey_idx ON ods.supplier(s_suppkey);

CREATE INDEX s_nationkey_idx ON ods.supplier (s_nationkey);

ANALYZE ods.supplier;

-- 4. Сравните время выполнения запросов с индексами и без.

-- Результаты и после сбора статистики и добавления индексов 

-- Query 1: Retrieve Customer Orders with Order and Customer Details
START QUERY  | 2026-04-25 23:02:00.843276
FINISH QUERY | 2026-04-25 23:02:01.191391
DURATION QUERY 0.35 SECONDS
-- Чаще всего ~ 0.35 секунд

Hash Join  (cost=0.00..2054.08 rows=301346 width=214) (actual time=15.896..503.669 rows=300000 loops=1)
  Hash Cond: (orders.o_custkey = customer.c_custkey)
  Extra Text: Hash chain length 1.2 avg, 5 max, using 24032 of 65536 buckets.
  ->  Gather Motion 1:1  (slice1; segments: 1)  (cost=0.00..772.64 rows=300000 width=111) (actual time=0.006..405.004 rows=300000 loops=1)
        ->  Sequence  (cost=0.00..451.62 rows=300000 width=111) (actual time=0.766..369.490 rows=300000 loops=1)
              ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                    Partitions selected: 522 (out of 522)
              ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..451.62 rows=300000 width=111) (actual time=0.727..305.950 rows=300000 loops=1)
                    Partitions scanned:  522 (out of 522) .
  ->  Hash  (cost=907.45..907.45 rows=30000 width=107) (actual time=15.831..15.831 rows=30000 loops=1)
        Buckets: 65536  Batches: 1  Memory Usage: 4237kB
        ->  Hash Join  (cost=0.00..907.45 rows=30000 width=107) (actual time=1.494..8.888 rows=30000 loops=1)
              Hash Cond: (customer.c_nationkey = nation.n_nationkey)
              Extra Text: Hash chain length 1.0 avg, 1 max, using 25 of 131072 buckets.
              ->  Gather Motion 1:1  (slice2; segments: 1)  (cost=0.00..458.42 rows=30000 width=85) (actual time=0.005..3.349 rows=30000 loops=1)
                    ->  Sequence  (cost=0.00..433.84 rows=30000 width=85) (actual time=0.412..5.324 rows=30000 loops=1)
                          ->  Partition Selector for customer (dynamic scan id: 2)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                                Partitions selected: 6 (out of 6)
                          ->  Dynamic Seq Scan on customer (dynamic scan id: 2)  (cost=0.00..433.84 rows=30000 width=85) (actual time=0.404..4.239 rows=30000 loops=1)
                                Partitions scanned:  6 (out of 6) .
              ->  Hash  (cost=431.01..431.01 rows=25 width=30) (actual time=1.374..1.374 rows=25 loops=1)
                    Buckets: 131072  Batches: 1  Memory Usage: 2kB
                    ->  Gather Motion 1:1  (slice3; segments: 1)  (cost=0.00..431.01 rows=25 width=30) (actual time=1.368..1.371 rows=25 loops=1)
                          ->  Seq Scan on nation  (cost=0.00..431.00 rows=25 width=30) (actual time=0.014..0.016 rows=25 loops=1)
Planning time: 346.448 ms
  (slice0)    Executor memory: 10292K bytes.  Work_mem: 4237K bytes max.
  (slice1)    Executor memory: 514717K bytes (seg0).
  (slice2)    Executor memory: 4474K bytes (seg0).
  (slice3)    Executor memory: 268K bytes (seg0).
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 545.782 ms

-- Query 2: Retrieve Detailed Order Information with Line Items
START QUERY  | 2026-04-25 23:05:52.68452
FINISH QUERY | 2026-04-25 23:05:53.604256
DURATION QUERY 0.92 SECONDS
-- Чаще всего < 0.95 секунд

Hash Join  (cost=0.00..4528.08 rows=1199410 width=212) (actual time=391.642..1229.589 rows=1199969 loops=1)
  Hash Cond: (lineitem.l_orderkey = orders.o_orderkey)
  Extra Text: Hash chain length 2.5 avg, 11 max, using 117742 of 131072 buckets.
  ->  Gather Motion 1:1  (slice1; segments: 1)  (cost=0.00..1827.25 rows=1199969 width=113) (actual time=0.007..326.526 rows=1199969 loops=1)
        ->  Sequence  (cost=0.00..520.10 rows=1199969 width=113) (actual time=1.450..463.147 rows=1199969 loops=1)
              ->  Partition Selector for lineitem (dynamic scan id: 2)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                    Partitions selected: 87 (out of 87)
              ->  Dynamic Seq Scan on lineitem (dynamic scan id: 2)  (cost=0.00..520.10 rows=1199969 width=113) (actual time=1.438..420.348 rows=1199969 loops=1)
                    Partitions scanned:  87 (out of 87) .
  ->  Hash  (cost=761.07..761.07 rows=300000 width=107) (actual time=391.529..391.529 rows=300000 loops=1)
        Buckets: 131072  Batches: 1  Memory Usage: 42808kB
        ->  Gather Motion 1:1  (slice2; segments: 1)  (cost=0.00..761.07 rows=300000 width=107) (actual time=2.234..360.032 rows=300000 loops=1)
              ->  Sequence  (cost=0.00..451.62 rows=300000 width=107) (actual time=0.867..297.195 rows=300000 loops=1)
                    ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                          Partitions selected: 522 (out of 522)
                    ->  Dynamic Seq Scan on orders (dynamic scan id: 1)  (cost=0.00..451.62 rows=300000 width=107) (actual time=0.818..286.166 rows=300000 loops=1)
                          Partitions scanned:  522 (out of 522) .
Planning time: 170.732 ms
  (slice0)    Executor memory: 75093K bytes.  Work_mem: 42808K bytes max.
  (slice1)    Executor memory: 163138K bytes (seg0).
  (slice2)    Executor memory: 464577K bytes (seg0).
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 1267.941 ms

-- Query 3: Retrieve Supplier and Part Information for Each Supplier-Part Relationship
START QUERY  | 2026-04-25 23:07:59.801268
FINISH QUERY | 2026-04-25 23:07:59.866423
DURATION QUERY 0.07 SECONDS
-- Чаще всего ~ 0.95 секунд

Hash Join  (cost=0.00..2583.00 rows=160000 width=432) (actual time=24.490..97.404 rows=160000 loops=1)
  Hash Cond: (partsupp.ps_suppkey = supplier.s_suppkey)
  Extra Text: Hash chain length 1.0 avg, 2 max, using 1944 of 32768 buckets.
  ->  Hash Join  (cost=0.00..1436.02 rows=160000 width=269) (actual time=18.515..68.002 rows=160000 loops=1)
        Hash Cond: (partsupp.ps_partkey = part.p_partkey)
        Extra Text: Hash chain length 1.7 avg, 8 max, using 23026 of 32768 buckets.
        ->  Gather Motion 1:1  (slice1; segments: 1)  (cost=0.00..642.50 rows=160000 width=143) (actual time=0.007..15.150 rows=160000 loops=1)
              ->  Seq Scan on partsupp  (cost=0.00..444.82 rows=160000 width=143) (actual time=0.190..14.941 rows=160000 loops=1)
        ->  Hash  (cost=479.10..479.10 rows=40000 width=130) (actual time=18.479..18.479 rows=40000 loops=1)
              Buckets: 32768  Batches: 1  Memory Usage: 6654kB
              ->  Gather Motion 1:1  (slice2; segments: 1)  (cost=0.00..479.10 rows=40000 width=130) (actual time=0.003..16.414 rows=40000 loops=1)
                    ->  Seq Scan on part  (cost=0.00..434.17 rows=40000 width=130) (actual time=0.330..4.561 rows=40000 loops=1)
  ->  Hash  (cost=843.26..843.26 rows=2000 width=167) (actual time=5.937..5.937 rows=2000 loops=1)
        Buckets: 32768  Batches: 1  Memory Usage: 399kB
        ->  Gather Motion 1:1  (slice4; segments: 1)  (cost=0.00..843.26 rows=2000 width=167) (actual time=1.210..5.684 rows=2000 loops=1)
              ->  Nested Loop  (cost=0.00..840.99 rows=2000 width=167) (actual time=0.763..4.801 rows=2000 loops=1)
                    Join Filter: true
                    ->  Broadcast Motion 1:1  (slice3; segments: 1)  (cost=0.00..431.04 rows=25 width=30) (actual time=0.240..0.243 rows=25 loops=1)
                          ->  Seq Scan on nation  (cost=0.00..431.00 rows=25 width=30) (actual time=0.020..0.025 rows=25 loops=1)
                    ->  Bitmap Heap Scan on supplier  (cost=0.00..408.26 rows=80 width=141) (actual time=0.021..0.178 rows=80 loops=25)
                          Recheck Cond: (s_nationkey = nation.n_nationkey)
                          ->  Bitmap Index Scan on s_nationkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.000..0.002 rows=80 loops=25)
                                Index Cond: (s_nationkey = nation.n_nationkey)
Planning time: 46.564 ms
  (slice0)    Executor memory: 19858K bytes.  Work_mem: 6654K bytes max.
  (slice1)    Executor memory: 608K bytes (seg0).
  (slice2)    Executor memory: 992K bytes (seg0).
  (slice3)    Executor memory: 268K bytes (seg0).
  (slice4)    Executor memory: 5329K bytes (seg0).  Work_mem: 9K bytes max.
  (slice5)    
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 109.970 ms

-- Query 4: Retrieve Comprehensive Customer Order and Line Item Details
START QUERY  | 2026-04-25 23:10:30.955167
FINISH QUERY | 2026-04-25 23:10:31.735443
DURATION QUERY 0.78 SECONDS
-- Чаще всего ~ 0.78 секунд

-- В плане запроса мы видим, что появилось обращение к индексу
Hash Join  (cost=0.00..1724.27 rows=61 width=286) (actual time=409.484..473.483 rows=45 loops=1)
  Hash Cond: (orders.o_custkey = customer.c_custkey)
  Extra Text: Hash chain length 1.0 avg, 1 max, using 1 of 65536 buckets.
  ->  Gather Motion 1:1  (slice2; segments: 1)  (cost=0.00..938.25 rows=61 width=216) (actual time=406.569..470.490 rows=45 loops=1)
        ->  Nested Loop  (cost=0.00..938.17 rows=61 width=216) (actual time=239.791..472.905 rows=45 loops=1)
              Join Filter: true
              ->  Broadcast Motion 1:1  (slice1; segments: 1)  (cost=0.00..398.18 rows=16 width=111) (actual time=209.699..209.706 rows=13 loops=1)
                    ->  Sequence  (cost=0.00..398.09 rows=16 width=111) (actual time=9.278..208.566 rows=13 loops=1)
                          ->  Partition Selector for orders (dynamic scan id: 1)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                                Partitions selected: 522 (out of 522)
                          ->  Dynamic Bitmap Heap Scan on orders (dynamic scan id: 1)  (cost=0.00..398.09 rows=16 width=111) (actual time=9.240..208.521 rows=13 loops=1)
                                Recheck Cond: (o_custkey = 22)
                                Heap Blocks: exact=93877344824072 lossy=93877301143624
                                ->  Dynamic Bitmap Index Scan on o_custkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.000..0.004 rows=0 loops=522)
                                      Index Cond: (o_custkey = 22)
              ->  Sequence  (cost=0.00..539.92 rows=4 width=113) (actual time=2.314..20.243 rows=3 loops=13)
                    ->  Partition Selector for lineitem (dynamic scan id: 2)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                          Partitions selected: 87 (out of 87)
                    ->  Dynamic Bitmap Heap Scan on lineitem (dynamic scan id: 2)  (cost=0.00..539.92 rows=4 width=113) (actual time=2.312..20.239 rows=3 loops=13)
                          Recheck Cond: (l_orderkey = orders.o_orderkey)
                          Heap Blocks: exact=93877301152888 lossy=93877301168248
                          ->  Dynamic Bitmap Index Scan on l_orderkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.000..0.003 rows=0 loops=1131)
                                Index Cond: (l_orderkey = orders.o_orderkey)
  ->  Hash  (cost=785.94..785.94 rows=2 width=74) (actual time=2.848..2.848 rows=1 loops=1)
        Buckets: 65536  Batches: 1  Memory Usage: 1kB
        ->  Gather Motion 1:1  (slice4; segments: 1)  (cost=0.00..785.94 rows=2 width=74) (actual time=2.846..2.846 rows=1 loops=1)
              ->  Nested Loop  (cost=0.00..785.94 rows=2 width=74) (actual time=2.426..2.434 rows=1 loops=1)
                    Join Filter: true
                    ->  Redistribute Motion 1:1  (slice3; segments: 1)  (cost=0.00..397.97 rows=2 width=52) (actual time=2.162..2.162 rows=1 loops=1)
                          Hash Key: customer.c_nationkey
                          ->  Sequence  (cost=0.00..397.97 rows=2 width=52) (actual time=0.576..1.206 rows=1 loops=1)
                                ->  Partition Selector for customer (dynamic scan id: 3)  (cost=10.00..100.00 rows=100 width=4) (never executed)
                                      Partitions selected: 6 (out of 6)
                                ->  Dynamic Bitmap Heap Scan on customer (dynamic scan id: 3)  (cost=0.00..397.97 rows=2 width=52) (actual time=0.570..1.200 rows=1 loops=1)
                                      Recheck Cond: (c_custkey = 22)
                                      Heap Blocks: exact=93877344818792 lossy=93877301083672
                                      ->  Dynamic Bitmap Index Scan on c_custkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.001..0.007 rows=0 loops=6)
                                            Index Cond: (c_custkey = 22)
                    ->  Bitmap Heap Scan on nation  (cost=0.00..387.97 rows=1 width=26) (actual time=0.263..0.270 rows=1 loops=1)
                          Recheck Cond: (n_nationkey = customer.c_nationkey)
                          ->  Bitmap Index Scan on n_nationkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.009..0.009 rows=1 loops=1)
                                Index Cond: (n_nationkey = customer.c_nationkey)
Planning time: 508.763 ms
  (slice0)    Executor memory: 884K bytes.  Work_mem: 1K bytes max.
  (slice1)    Executor memory: 93593K bytes (seg0).  Work_mem: 9K bytes max.
  (slice2)    Executor memory: 326961K bytes (seg0).  Work_mem: 9K bytes max.
  (slice3)    Executor memory: 1592K bytes (seg0).  Work_mem: 9K bytes max.
  (slice4)    Executor memory: 616K bytes (seg0).  Work_mem: 9K bytes max.
  (slice5)    
  (slice6)    
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 480.439 ms

-- Query 5: Retrieve All Parts Supplied by a Specific Supplier with Supplier Details
START QUERY  | 2026-04-25 23:14:20.716038
FINISH QUERY | 2026-04-25 23:14:20.780633
DURATION QUERY 0.06 SECONDS
-- Чаще всего ~ 0.06 секунд

-- В плане запроса мы видим, что появилось обращение к индексу
Hash Join  (cost=0.00..1558.63 rows=88 width=228) (actual time=13.862..19.373 rows=80 loops=1)
  Hash Cond: (partsupp.ps_suppkey = supplier.s_suppkey)
  Extra Text: Hash chain length 1.0 avg, 1 max, using 1 of 32768 buckets.
  ->  Gather Motion 1:1  (slice2; segments: 1)  (cost=0.00..782.60 rows=88 width=134) (actual time=11.459..16.928 rows=80 loops=1)
        ->  Nested Loop  (cost=0.00..782.52 rows=88 width=134) (actual time=2.889..18.838 rows=80 loops=1)
              Join Filter: true
              ->  Redistribute Motion 1:1  (slice1; segments: 1)  (cost=0.00..388.54 rows=88 width=8) (actual time=2.311..2.317 rows=80 loops=1)
                    Hash Key: partsupp.ps_partkey
                    ->  Bitmap Heap Scan on partsupp  (cost=0.00..388.53 rows=88 width=8) (actual time=0.501..1.151 rows=80 loops=1)
                          Recheck Cond: (ps_suppkey = 2)
                          ->  Bitmap Index Scan on ps_suppkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.093..0.093 rows=80 loops=1)
                                Index Cond: (ps_suppkey = 2)
              ->  Bitmap Heap Scan on part  (cost=0.00..393.92 rows=1 width=130) (actual time=0.007..0.206 rows=1 loops=80)
                    Recheck Cond: (p_partkey = partsupp.ps_partkey)
                    ->  Bitmap Index Scan on p_partkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.000..0.002 rows=1 loops=80)
                          Index Cond: (p_partkey = partsupp.ps_partkey)
  ->  Hash  (cost=775.94..775.94 rows=1 width=98) (actual time=2.358..2.358 rows=1 loops=1)
        Buckets: 32768  Batches: 1  Memory Usage: 1kB
        ->  Gather Motion 1:1  (slice4; segments: 1)  (cost=0.00..775.94 rows=1 width=98) (actual time=2.355..2.355 rows=1 loops=1)
              ->  Nested Loop  (cost=0.00..775.94 rows=1 width=98) (actual time=0.902..0.911 rows=1 loops=1)
                    Join Filter: true
                    ->  Redistribute Motion 1:1  (slice3; segments: 1)  (cost=0.00..387.97 rows=1 width=76) (actual time=0.611..0.611 rows=1 loops=1)
                          Hash Key: supplier.s_nationkey
                          ->  Bitmap Heap Scan on supplier  (cost=0.00..387.97 rows=1 width=76) (actual time=0.482..0.497 rows=1 loops=1)
                                Recheck Cond: (s_suppkey = 2)
                                ->  Bitmap Index Scan on s_suppkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.009..0.009 rows=1 loops=1)
                                      Index Cond: (s_suppkey = 2)
                    ->  Bitmap Heap Scan on nation  (cost=0.00..387.97 rows=1 width=26) (actual time=0.288..0.296 rows=1 loops=1)
                          Recheck Cond: (n_nationkey = supplier.s_nationkey)
                          ->  Bitmap Index Scan on n_nationkey_idx  (cost=0.00..0.00 rows=0 width=0) (actual time=0.008..0.008 rows=1 loops=1)
                                Index Cond: (n_nationkey = supplier.s_nationkey)
Planning time: 49.718 ms
  (slice0)    Executor memory: 689K bytes.  Work_mem: 1K bytes max.
  (slice1)    Executor memory: 1238K bytes (seg0).  Work_mem: 41K bytes max.
  (slice2)    Executor memory: 13985K bytes (seg0).  Work_mem: 9K bytes max.
  (slice3)    Executor memory: 1369K bytes (seg0).  Work_mem: 9K bytes max.
  (slice4)    Executor memory: 616K bytes (seg0).  Work_mem: 9K bytes max.
  (slice5)    
  (slice6)    
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 27.391 ms


-- 4.1. Cоберите информацию по использованию индексов из системных таблиц и представлений.

SELECT * FROM pg_class -- Содерджит описания объектов БД 
WHERE relkind = 'i'; -- relkind = 'i' фильтрация по индексам

SELECT * FROM pg_index -- Содержит информацию об индексах

SELECT * FROM pg_stat_user_indexes -- Содержит информация о пользовательских индексах

SELECT * FROM pg_statistic -- Статистические данных о содержимом таблиц, созданные командой ANALYZE

--Запрос возвращает список индексов схемы ods
SELECT
	n.nspname AS schema_name
	, c.relname AS table_name
	, i.relname AS index_name
	, pg_get_indexdef(i.oid) AS index_definition
	, indisunique AS is_unique
	, indisprimary AS is_primary
FROM
	pg_class i
JOIN pg_index ix ON
	i.oid = ix.indexrelid
JOIN pg_class c ON
	ix.indrelid = c.oid
JOIN pg_namespace n ON
	c.relnamespace = n.oid
WHERE
	i.relkind = 'i'
	AND n.nspname = 'ods';

-- 5. Сделайте вывод о необходимости индексов в Greenplum.

-- На мой взгляд в Greenplum оправдано примененеие индексов с случае выполнения запросов возвращающих небольшое (одну или несколько строк) количество данных с фильтрацией по индексируемым столбцам. 
-- Ещё индексы могут улучшить производительность запросов к сжатым таблицам оптимизированным для прилолжений, так как планировщик может обращаться напрямую к индексу вместо полного сканирования таблицы.