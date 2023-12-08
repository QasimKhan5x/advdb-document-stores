SET CLIENT_ENCODING TO 'UTF8';

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS sf1;

CREATE TEMPORARY TABLE t(data jsonb);
CREATE TABLE sf1(data jsonb);

\timing
-- Copying data from JSON file to temporary table
copy t from 'C:\Users\Hareem Raza\Documents\BDMA Sem1\Advanced DB\Project\Implementation\Benchmark\data\twitter_sf_1.json' csv quote e'\x01' delimiter e'\x02';

-- Inserting data from temporary table to permanent table
insert into sf1
select * from t;
