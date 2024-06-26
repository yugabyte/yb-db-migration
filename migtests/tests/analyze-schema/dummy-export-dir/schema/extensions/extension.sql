-- setting variables for current session
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE EXTENSION IF NOT EXISTS aws_commons WITH SCHEMA public;


CREATE EXTENSION IF NOT EXISTS plperl WITH SCHEMA pg_catalog;


CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;


CREATE EXTENSION IF NOT EXISTS hstore_plperl WITH SCHEMA public;


CREATE EXTENSION IF NOT EXISTS pg_visibility WITH SCHEMA public;

CREATE EXTENSION IF NOT EXISTS hll WITH SCHEMA public;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;