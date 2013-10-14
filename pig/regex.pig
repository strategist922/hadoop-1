a = load 'Node_Meas_Outflows_Test.csv' using PigStorage ( ';' ) as ( nodeid:chararray, consumer:chararray, pstart:chararray, pend:chararray, p:DOUBLE );
b = FOREACH  a GENERATE nodeid, consumer, REGEX_EXTRACT(pstart, '((19|20)\\d{2})', 1) as ystart, REGEX_EXTRACT(pend, '((19|20)\\d{2})', 1) as yend, pstart, pend, p;
c = limit b 10;
dump c; 
