customers = LOAD '/Project/Customers.csv' USING PigStorage(',') AS (id, name, age, code, salary);
transactions = LOAD '/Project/Transactions.csv' USING PigStorage(',') AS (transid, custid, total, numitems, description);

A = JOIN transactions BY custid, customers BY id using 'replicated';
B = GROUP A BY (id, name);
C = FOREACH B GENERATE group.name, COUNT(A) as numtrans;
D = ORDER C BY numtrans ASC;

first = LIMIT D 1;
E = FILTER D BY numtrans == first.numtrans; 

STORE E INTO '/Project/output4';
