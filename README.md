# IMPLEMENTATION OF DISTRIBUTED DATABASE MANAGEMENT SYSTEM

The main purpose of the project is to build a commercial solution to manage a Relational database.
This involves implementing the logics behind the MySQL queries using appropriate data structures in
Java programming language. The basic queries need to be handled by the system accordingly. It also
should facilitate the environment of query execution in the distributed environment. Apart from the
execution of normal MySQL queries, it should handle the transaction management which also happens
in a distributed way. It should facilitate options to recover database in case of migrating data and
should display the ERD diagram or reverse engineering of the existing database.


In the proposed model, the database is considered the directory and tables are considered as files
inside the directory. The records are stored by a $. There is a meta file stored in the root directory
called Database and it has a meta file which marks the status of table location and locking status. We
had the global data dictionary in the root folder synced across various devices to achieve the
distributed environment.
