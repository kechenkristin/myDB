## MYDB(Kechen's Database)
Kechen's implementation of a relational database, skeleton codes forked from MIT 6.830.

MyDB supports these features:
- Storage and Bufferpool Management: Implemented efficient storage and bufferpool management, including an LRU Replacement Policy, to optimize data retrieval and storage architecture.
- Query Execution: Implemented Sequential Scan, Filer, Join, Aggregation, Insert, and Deletion Operator using Iterator.
- Optimization: Applied cost-based optimization techniques, improving query efficiency by calculating cardinality and selectivity.
- Indexing with B+ Tree: Employed B+ tree indexing to enhance data access performance, ensuring rapid retrieval of data.
- Transaction Control: Achieved the ACID features of transactions through the writer-reader model and Two-Phase Locking. Implemented a timestamp-based Deadlock detection algorithm.
- Logging: Implemented STEAL and NON-FORCE policies, enabling checkpoint, rollback, and recovery features.

## How to run local Junit test

settings -> Project structure -> Project settings -> SDK
Specify a lower version SDK(i.e. java 20)

## How to run ANT build
settings -> Project structure -> Project settings -> SDK
Specify a lower version SDK(i.e. java 16)

in ant build, here says the java version:

```
 <property name="sourceversion" value="1.8"/>
```


## How to use simpleDB

1. Build jar  
   use ant build to build the jar package
2. Once get the jar, rememer to config IDEA rather than use command line!


Reference from lab2.md: 

We've provided you with a query parser for SimpleDB that you can use to write and run SQL queries against your database
once you have completed the exercises in this lab.

The first step is to create some data tables and a catalog. Suppose you have a file `data.txt` in side the data folder with the following
contents:

```
1,10
2,20
3,30
4,40
5,50
5,50
```

You can convert this into a SimpleDB table using the
`convert` command (make sure to type <tt>ant</tt> first!):

```
java -jar dist/simpledb.jar convert data/data.txt 2 "int,int"
```

This creates a file `data.dat`. In addition to the table's raw data, the two additional parameters specify that each
record has two fields and that their types are `int` and
`int`.

Next, create a catalog file, `catalog.txt`, with the following contents:

```
data (f1 int, f2 int)
```

This tells SimpleDB that there is one table, `data` (stored in
`data.dat`) with two integer fields named `f1`
and `f2`.

Finally, invoke the parser. You must run java from the command line (ant doesn't work properly with interactive
targets.)
From the `simpledb/` directory, type:

```
java -jar dist/simpledb.jar parser data/catalog.txt
```

You should see output like:

```
Added table : data with schema INT(f1), INT(f2), 
SimpleDB> 
```

Finally, you can run a query:

```
SimpleDB> select d.f1, d.f2 from data d;
Started a new transaction tid = 1221852405823
 ADDING TABLE d(data) TO tableMap
     TABLE HAS  tupleDesc INT(d.f1), INT(d.f2), 
1       10
2       20
3       30
4       40
5       50
5       50

 6 rows.
----------------
0.16 seconds

SimpleDB> 
```

## Resources
My study notes for CMU 15445:

https://drive.google.com/file/d/14RguZaOPNdGTMTlmW-96zsXMiqnbwPeT/view?usp=drive_link


