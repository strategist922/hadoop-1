c = load '/Node_Meas_Outflows_2012_2013.csv' USING PigStorage(';') as ( nodeid:chararray, consumer:chararray, pstart:chararray, pend:chararray, p:double );
h = foreach c generate nodeid, consumer, pstart, SUBSTRING( pstart, 0,  10 ) as pstartyear, SUBSTRING( pstart, 11, 19   ) as pstarttime, SUBSTRING( pend, 0,  10 ) as pendyear, SUBSTRING( pend, 11, 19   ) as pendtime. p;
STORE h into Node_Meas_Outflows_hive USING PigStorage( ',' );
