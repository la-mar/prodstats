create database well;
create user prodstats  with password'PASSWORD_HERE';
alter user prodstats with superuser; -- required for migrations to create postgis extension
grant all on database well to prodstats;
grant all privileges on database well to prodstats;
grant usage on schema public to prodstats;
grant all privileges on all tables in schema public to prodstats;
grant all privileges on all sequences in schema public to prodstats;
alter default privileges in schema public grant all on tables to prodstats;
alter default privileges in schema public grant all on sequences to prodstats;
