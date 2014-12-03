sql-to-mongodb
==============

Java tool to convert SQL table to MongoDB collection

Easly convert MSSQL tables to MongoDB.

* Download SQL Server JDBC driver from http://msdn.microsoft.com/en-us/sqlserver/aa937724.aspx
 Put the SQLJDBC4.jar file in the same folder.

 Specify TABLE NAME and NUMBER OF ROWS at a time to import.
 
  java -classpath sqljdbc4.jar:json.jar:mongo-java-driver-2.12.4.jar:. MSSql_Table_To_MongoDB_Collection mssql_table_name number_of_rows_at_a_time


