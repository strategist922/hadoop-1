--load pandadate into pig
--pls note I used floats in the beginning, but encountered some casting and conversion problems.
c = load 'asv:///pandadata2012.csv' USING PigStorage('\t') as ( consumer:long, dato:chararray, direction,unit,timespan, 
t1:double,t2:double,t3:double,t4:double,t5:double,t6:double,t7:double,t8:double,t9:double,t10:double,t11:double,t12:double,t13:double,t14:double,t15:double,t16:double,t17:double,t18:double,t19:double,t20:double,t21:double,t22:double,t23:double,t24:double);


--We need to pivot the table. Might be a smarter way to do this (ie scooping the data differently would be the smartest thing to do)


c1 = FOREACH c GENERATE consumer, CONCAT(REPLACE(dato, '/','-'), ' 00:00') as dato, direction, t1 as consumption;
c2 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 01:00') as dato, direction, t2 as consumption;
c3 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 02:00') as dato, direction, t3 as consumption;
c4 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 03:00') as dato, direction, t4 as consumption;
c5 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 04:00') as dato, direction, t5 as consumption;
c6 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 05:00') as dato, direction, t6 as consumption;
c7 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 06:00') as dato, direction, t7 as consumption;
c8 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 07:00') as dato, direction, t8 as consumption;
c9 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 08:00') as dato, direction, t9 as consumption;
c10 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 09:00') as dato, direction, t10 as consumption;
c11 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 10:00') as dato, direction, t11 as consumption;
c12 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 11:00') as dato, direction, t12 as consumption;
c13 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 12:00') as dato, direction, t13 as consumption;
c14 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 13:00') as dato, direction, t14 as consumption;
c15 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 14:00') as dato, direction, t15 as consumption;
c16 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 15:00') as dato, direction, t16 as consumption;
c17 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 16:00') as dato, direction, t17 as consumption;
c18 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 17:00') as dato, direction, t18 as consumption;
c19 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 18:00') as dato, direction, t19 as consumption;
c20 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 19:00') as dato, direction, t20 as consumption;
c21 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 20:00') as dato, direction, t21 as consumption;
c22 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 21:00') as dato, direction, t22 as consumption;
c23 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 22:00') as dato, direction, t23 as consumption;
c24 = FOREACH c generate consumer, CONCAT(REPLACE(dato, '/','-'), ' 23:00') as dato, direction, t24 as consumption;


--Making a new table, by joining all the pivoted tables 
c_all = UNION c1,c2,c3,c4,c5,c6,c7,c8,c9,c10, c11, c12,c13,c14, c15,c16, c17, c18,c19, c20,c21,c22,c23,c24;

--Load the weather
t = load 'asv:///WeatherTemp.csv' using PigStorage(';') as( temp:double, dato:chararray );
--join the temperature and consumpotion
c_all_temp = join c_all by dato, t by dato using 'replicated';

STORE c_all_temp into 'panda_data' USING PigStorage(',');
