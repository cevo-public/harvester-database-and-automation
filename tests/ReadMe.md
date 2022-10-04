## Automated testing

Test suite for some components of the code base, largely incomplete.

Most importantly, the two scripts `database_dump.py` and
`database_setup.py` let you set up a test database instance locally,
based on data partially dumped from what is currently in production.

Requires [PostgreSQL] to be installed. Its command-line tools `psql`
and `pg_dump` must be available on the executable search path.

The configuration file for the database connection, `database.yaml`,
is distributed separately and must be placed in this folder here. It
should look something like this:
```yaml
production:
  name:     "database_name"
  host:     "server.ethz.ch"
  port:     NULL
  user:     "technical_user"
  password: "do_not_expose"

test:
  name:     "test"
```

If that file doesn't contain the password for the technical user (and
perhaps it shouldn't), then that password can be entered at the prompt.

Note that the test instance can usually be set up in a way that it
doesn't require any log-in credentials, though it's not necessary to
do that. If not specified, the test suite will pass `None` for the
missing values when trying to connect to the database instance. An
explicit value of `NULL` in YAML does the same.


[PostgreSQL]: https://www.postgresql.org/download
