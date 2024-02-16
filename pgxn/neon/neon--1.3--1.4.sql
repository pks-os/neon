\echo Use "ALTER EXTENSION neon UPDATE TO '1.4'" to load this file. \quit

CREATE FUNCTION wal_log_file(path text)
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'wal_log_file'
LANGUAGE C STRICT PARALLEL UNSAFE;
