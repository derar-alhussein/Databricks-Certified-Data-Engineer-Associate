CREATE DATABASE IF NOT EXISTS hive_metastore.hr_db
LOCATION 'dbfs:/mnt/demo/hr_db.db';

CREATE TABLE hive_metastore.hr_db.employees (id INT, name STRING, salary DOUBLE, city STRING);

INSERT INTO hive_metastore.hr_db.employees
VALUES (1, "Anna", 2500, "Paris"),
       (2, "Thomas", 3000, "London"),
       (3, "Bilal", 3500, "Paris"),
       (4, "Maya", 2000, "Paris"),
       (5, "Sophie", 2500, "London"),
       (6, "Adam", 3500, "London"),
       (7, "Ali", 3000, "Paris");

CREATE VIEW hive_metastore.hr_db.paris_emplyees_vw
AS SELECT * FROM hive_metastore.hr_db.employees WHERE city = 'Paris';

------------------------------------------------------

-- grant privileges to hr_team on hive_metastore.hr_db to read, modify and read medata

-- grant privileges to a user email account on hive_metastore.hr_db to read, modify and read medata

-- show existing grants

-- revoke provided grants
