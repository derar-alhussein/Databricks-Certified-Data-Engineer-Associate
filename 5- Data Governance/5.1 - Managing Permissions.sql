CREATE CATALOG hr_catalog

CREATE SCHEMA IF NOT EXISTS hr_catalog.hr_db;

CREATE TABLE hr_catalog.hr_db.employees (id INT, name STRING, salary DOUBLE, city STRING);

INSERT INTO hr_catalog.hr_db.employees
VALUES (1, "Anna", 2500, "Paris"),
       (2, "Thomas", 3000, "London"),
       (3, "Bilal", 3500, "Paris"),
       (4, "Maya", 2000, "Paris"),
       (5, "Sophie", 2500, "London"),
       (6, "Adam", 3500, "London"),
       (7, "Ali", 3000, "Paris");

CREATE VIEW hr_catalog.hr_db.paris_emplyees_vw
AS SELECT * FROM hr_catalog.hr_db.employees WHERE city = 'Paris';

------------------------------------------------------

GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA hr_db TO hr_team;
GRANT USE SCHEMA ON SCHEMA hr_catalog.hr_db TO hr_team;
GRANT USE CATALOG ON CATALOG hr_catalog TO hr_team;
SHOW GRANTS ON SCHEMA hr_catalog.hr_db;

GRANT CREATE SCHEMA ON CATALOG hr_catalog TO hr_team ;
SHOW GRANTS ON CATALOG hr_catalog;

GRANT SELECT ON TABLE hr_catalog.hr_db.employees TO `account users`;
SHOW GRANTS ON VIEW hr_catalog.hr_db.employees;
GRANT USE SCHEMA ON SCHEMA hr_catalog.hr_db TO `account users`;
GRANT USE CATALOG ON CATALOG hr_catalog TO `account users`;

GRANT SELECT ON VIEW hr_catalog.hr_db.paris_emplyees_vw TO `adam@example.com`;
SHOW GRANTS ON VIEW hr_catalog.hr_db.paris_emplyees_vw;
GRANT USE SCHEMA ON SCHEMA hr_catalog.hr_db TO `adam@example.com`;
GRANT USE CATALOG ON CATALOG hr_catalog TO `adam@example.com`;

------------------------------------------------------

SELECT * FROM system.access.audit

