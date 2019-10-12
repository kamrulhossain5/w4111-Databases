Kamrul Hossain

kh2857

Homework 1: RDB and CSV Datatables

------------------------------------------------------------------------------------------------------------------------

To test the csv_table_tests file, just run csv_table_tests.py. To test the rdb_table_tests file, just run
rdb_table_tests.py. I used the Batting data for both tests. The edge cases I've discovered are if one of the field
values were null, if no values were included, if the dictionary was null, if there are duplicates already in the data
table, if there was an invalid query, if there are invalid key columns, and if the amount of key columns added in
doesn't match the amount of columns given from the start.

The way I structured my CSVDataTable file is that I had to read the csv file and then add each row into self._rows and
the way I structured my RDBDatatable is that I had to initialize the query and send that to the database server. I
included comments in the code to explain how I implemented each method, but everything else is straightforward and
follows the homework guidelines.