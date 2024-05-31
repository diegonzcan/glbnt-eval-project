-- HUMAN RESOURCES DATABASE DDL --
-- DATABASE CREATION --
CREATE DATABASE hrDB;
USE hrDB;
-- TABLES --
CREATE TABLE jobs (
	id INT NOT NULL AUTO_INCREMENT,
    job VARCHAR (50) NOT NULL,
    PRIMARY KEY (id)
);
CREATE TABLE departments (
	id INT NOT NULL AUTO_INCREMENT,
    deparment VARCHAR (30) NOT NULL,
    PRIMARY KEY (id)
);
CREATE TABLE hired_employees(
	id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR (30) NOT NULL,
    datetime DATETIME NOT NULL,
    department_id INT NOT NULL,
    job_id INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (job_id) REFERENCES jobs(id)
)

