-- List users
select *
from pg_catalog.pg_user;

-- List non-user and non-system roles
select *
from pg_catalog.pg_roles r
where
  rolname not like 'pg_%'
  and not exists(
    select
    from pg_catalog.pg_user u
    where r.rolname = u.usename
  );


-- Get all granted privileges
select
  grantor,
  grantee,
  table_name,
  string_agg(lower(privilege_type), ',') as privileges
from information_schema.role_table_grants
where
  table_schema = 'public'
  and grantee <> 'postgres'
group by grantor, grantee, table_name;


-- Get owner of tables
select tablename, tableowner
from pg_tables
where schemaname = 'public';


-- Create a new user
create user <user> password '<password>';


-- Change password
-- Every user can change the own password.
alter user <user> password '<password>';


-- Keep the authorization strict: Per default, the user should not be allowed to do anything, not even to connect to
-- our database.
revoke connect on database sars_cov_2 from public;
revoke all on all tables in schema public from public;


-- Give a user the privilege to connect to our database
grant connect on database sars_cov_2 to <user>;


-- Give a user read+write privileges to all existing tables
grant select, insert, update, delete, references
on all tables in schema public
to <user>;

grant create
on database sars_cov_2
to <user>;


-- Give a user read+write privileges to all future tables
alter default privileges
in schema public
grant select, insert, update, delete, references on tables to <user>;


-- Give a user read privileges to selected tables
grant select
on <table1>, <table2>
to <user>;
