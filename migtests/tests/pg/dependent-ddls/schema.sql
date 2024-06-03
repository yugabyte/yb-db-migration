-- this test covers the cases when ddl are dependent on each other and the order of execution during import schema might be wrong causing does not exist error
-- this has fixed as a part of enhancement in deferred list approach

CREATE FUNCTION public.lower(text[]) RETURNS text[]
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$
  SELECT 
    array_agg(tmp.value) 
  FROM 
    (SELECT lower(unnest($1)) AS value) tmp
$_$;


CREATE DOMAIN public.lower_array_text AS text[]
	CONSTRAINT lower_array_text_check CHECK ((VALUE = public.lower(VALUE)));


CREATE DOMAIN public.lower_text AS text
	CONSTRAINT lower_text_check CHECK ((VALUE = lower(VALUE)));


CREATE TABLE public.table1 (
    col1 public.lower_text NOT NULL,
    col2 public.lower_text,
    col3 public.lower_array_text,
    col4 timestamp with time zone DEFAULT statement_timestamp() NOT NULL
);

INSERT into public.table1(col1,col2,col3) values('a','b','{"c","d"}');
INSERT into public.table1(col1,col2,col3) values('e','f','{"g","h"}');


CREATE FUNCTION public.array_sort(anyarray) RETURNS anyarray
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$
  SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$_$;

CREATE TYPE public.enum_mail_hash_method_target_field AS ENUM (
    'SUBJECT',
    'EXTERNAL_SUBJECT',
    'FROM',
    'EXTERNAL_FROM',
    'TO',
    'EXTERNAL_TO',
    'CC',
    'EXTERNAL_CC',
    'BCC',
    'EXTERNAL_BCC',
    'DATE',
    'EXTERNAL_DATE',
    'BODY',
    'EXTERNAL_BODY',
    'ATTACHMENT_HASHES',
    'ATTACHMENT_COUNT',
    'ATTACHMENT_NAMES'
);

CREATE TABLE public.table2 (
    col1 integer NOT NULL,
    target_fields public.enum_mail_hash_method_target_field[] NOT NULL,
    created_at timestamp with time zone DEFAULT statement_timestamp() NOT NULL,
    CONSTRAINT target_fields_sorted CHECK ((target_fields = public.array_sort(target_fields)))
);
