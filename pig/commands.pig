create table panda ( consumer bigint, dato string, direction string, unit string, timespan int, t1 float, t2 float, t3 float, t4 float,  t5 float, t6 float, t7 float, t8 float, 

 t9 float, t10 float, t11 float, t12 float,t13 float, t14 float, t15 float, t16 float, t17 float, t18 float, t19 float, t20 float,t21 float, t22 float, t23 float, t24 float) 

ROW FORMAT DELIMITED FIELDS TERMINATED by '\t';



load data inpath
'asv://donghadoopcluster@donghadoop.blob.core.windows.net/pandadata.csv' overwrite into table panda


panda = load 'asv:///pandadata1.csv' as ( consumer, dato, direction,unit,timespan, t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17,t18,t19,t20,t21,t22,t23,t24);

grouped = group panda by consumer;

avg1 = foreach grouped generate group, AVG(panda.t1), AVG(panda.t2), AVG(panda.t3);


store avg into 'average_t1';
