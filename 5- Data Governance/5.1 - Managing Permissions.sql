CREATE DATABASE IF NOT EXISTS hr_db;

CREATE TABLE hr_db.employees (id INT, name STRING, salary DOUBLE, city STRING);

INSERT INTO hr_db.employees
VALUES (1, "Anna", 2500, "Paris"),
       (2, "Thomas", 3000, "London"),
       (3, "Bilal", 3500, "Paris"),
       (4, "Maya", 2000, "Paris"),
       (5, "Sophie", 2500, "London"),
       (6, "Adam", 3500, "London"),
       (7, "Ali", 3000, "Paris");

CREATE VIEW hr_db.paris_emplyees_vw
AS SELECT * FROM hr_db.employees WHERE city = 'Paris';

------------------------------------------------------

GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA hr_db TO hr_team;

GRANT USE SCHEMA ON SCHEMA hr_db TO hr_team;

GRANT SELECT ON VIEW hr_db.paris_emplyees_vw TO `adam@example.com`;

SHOW GRANTS ON SCHEMA hr_db;

SHOW GRANTS ON VIEW hr_db.paris_emplyees_vw;