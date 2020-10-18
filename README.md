# Werkspot-DE-Challenge
Data pipeline to create Fact,Dimensions and snapshot tables.

## 1) Python Libraries used:
a) pandas <br>
b) numpy <br>
c) re <br>
d) psycopg2 : To connect to postgres <br>
e) sqlalchemy : To convert pandas to postgres tables <br>

## 2) Database used:  
a) Postgresql version- 10.14 <br>
b) Schema  created: WERKSPOT_DB <br>
c) Connection details passed as parameters through config file <br>

## 3) Dimension model: 
Attached excel consisting the fact and dimensions created for Challenge 1. <br>

## 4) For Challenge 1:
Based on the dimensional model created,I have created a python script to read and convert the .csv file into necessary fact and dimension tables.I have applied the necessary transformations to convert the .csv into respective fact and dimensions and have loaded into postgres tables.

## 5) For Challenge 2: 
Based on the instructions provided in the word document, I have written a sql script which will refresh the AVAILABILITY_SNAPSHOT table and have called the query through a python script.
One assumption based on the point 2 of the note, if a professional_id=1 is became_able_to_propose at time '2020-01-01 10:00:00' and became_not_able_to_propose at time '2020-01-01 12:00:00' then he would be considered inactive for that day.


## 6) Aspects on which the code can be further improved:
a) Parameterize the path in code  on which the .csv is placed. <br>
b) Parameterize the connection details for postgres database. <br>
c) As of now the code drop creates the fact and dimensions tables. Code improvement can be done to incrementally load these tables. <br>
d) Primary key and foreign key constraints have not been applied at the table level. <br>
e) Security aspects such as having the correct permissions at table level can also be implemented. <br>
f) Error handling in the code to capture errors at various stages for example  while loading data into the table can also be implemented. <br>
