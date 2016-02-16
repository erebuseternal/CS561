customers = LOAD '/Project/Customers.csv' USING PigStorage(',') AS (id, name, age, code, salary);
transactions = LOAD '/Project/Transactions.csv' USING PigStorage(',') AS (transid, custid, total: float, numitems: int, description);

A = JOIN transactions BY custid, customers BY id USING 'replicated';
B = GROUP A BY (id, name);
C = FOREACH B GENERATE group.id, group.name, COUNT(A.transid), SUM(A.total), MAX(A.numitems);

STORE C INTO '/Project/output2';
