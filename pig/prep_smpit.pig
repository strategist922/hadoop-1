c = load '/Node_Meas_Outflows_2012_2013.csv' USING PigStorage(';') as ( nodeid:chararray, consumer:chararray, pstart:chararray, pend:chararray, p:double );
h = foreach c generate nodeid, consumer, pstart, SUBSTRING( pstart, 0,  10 ) as pstartyear, SUBSTRING( pstart, 11, 19   ) as pstarttime, SUBSTRING( pend, 0,  10 ) as pendyear, SUBSTRING( pend, 11, 19   ) as pendtime, p;
STORE h into 'Node_Meas_Outflows_hive' USING PigStorage( ',' );


**********************************************************************
c = load '/Node_Meas_Outflows_V1.csv' USING PigStorage(',') as ( nodeid:chararray, consumer:chararray, pstart, pend, p:double );

c1 = FOREACH c generate nodeid, consumer,  SUBSTRING(pstart,0,4) as startyear,  SUBSTRING(pend,0,4) as endyear, pstart, pend, p;

c2 = FOREACH c generate nodeid, consumer,  SUBSTRING(pstart,6,10) as startyear,  SUBSTRING(pend,6,10) as endyear, pstart, pend, p;

c1filter = FILTER c1  BY startyear == '2008' or startyear == '2009' or startyear == '2010'or startyear == '2011'or startyear == '2012'or startyear == '2013';

c2filter = FILTER c2  BY startyear == '2008' or startyear == '2009' or startyear == '2010'or startyear == '2011'or startyear == '2012'or startyear == '2013';

c3 = UNION c1filter, c2filter;

STORE c3 into '/user/admin/Node_Meas_Outflows_hive' USING PigStorage( ',' );
