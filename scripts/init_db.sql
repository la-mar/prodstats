create database prodstats;
create user prodstats;
grant all on database prodstats to prodstats;
grant all privileges on database prodstats to prodstats;
grant usage on schema public to prodstats;
grant all privileges on all tables in schema public to prodstats;
grant all privileges on all sequences in schema public to prodstats;
alter default privileges in schema public grant all on tables to prodstats;
alter default privileges in schema public grant all on sequences to prodstats;
