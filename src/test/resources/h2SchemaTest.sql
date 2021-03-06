DROP TABLE IF EXISTS gis_schema.schema_test;
DROP SCHEMA IF EXISTS gis_schema;
CREATE SCHEMA gis_schema;
CREATE TABLE gis_schema.schema_test (
"f1" integer,
"f2" boolean,
"f5" tinyint,
"f7" smallint,
"f8" bigint,
"f9" identity,
"f12" decimal,
"f13" double,
"f15" real,
"f16" time,
"f17" date,
"f18" timestamp,
"f19" binary,
"f20" other,
"f21" varchar,
"f25" varchar_ignorecase,
"f26" char,
"f27" blob,
"f28" clob);
