-- One PostgreSQL instance, one database + owner per service (no shared schemas between services).
CREATE USER camunda WITH PASSWORD 'camunda';
CREATE DATABASE camunda OWNER camunda;
CREATE USER applications WITH PASSWORD 'applications';
CREATE DATABASE applications OWNER applications;
