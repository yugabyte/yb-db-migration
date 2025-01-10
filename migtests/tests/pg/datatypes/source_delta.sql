INSERT INTO num_types(v1, v2, v3, v4, v5, v6)
VALUES (32767, 214748364, 922337203685477587, 123.456, 12345.6789, 123456.789);

UPDATE num_types
SET v1 = v1 * 0.5, v2 = v2 + 2, v3 = v3 * 2, v4 = v4 - 2, v5 = v5 * 2, v6 = v6 * 2;

UPDATE num_types
SET v1 = v1 - 100, v2 = v2 + 500, v3 = v3 - 1000, v5 = v5 + 5000, v6 = v6 - 10000.00::money;

DELETE FROM num_types WHERE v3 < 0;

insert into decimal_types values(0, 987455334362780682465462748789243337501610978301813276850553121352052192654789289113097427358778598.34278992, 12367890123456789.12);

UPDATE decimal_types
SET n1 = n1 + 1000, n2 = n2 - 100;

UPDATE decimal_types
SET n1 = n1 - 5012233222233.332220, n2 = n2 + 50;

INSERT INTO datatypes1(bool_type, char_type1, varchar_type, byte_type, enum_type)
VALUES (TRUE, 'A', 'Hello, world!', E'\\xDEABDAEF', 'Mon');

UPDATE datatypes1
SET bool_type = FALSE, char_type1 = 'X', varchar_type = 'Updated!', enum_type = 'Sun' where id=1;

DELETE FROM datatypes1 WHERE enum_type = 'Fri';

INSERT INTO datetime_type(v1, v2, v3, v4)
VALUES ('2024-02-07', '15:30:00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO datetime_type(v1, v2, v3, v4)
VALUES ('2024-03-15', '12:45:00', CURRENT_TIMESTAMP - INTERVAL '1 day', CURRENT_TIMESTAMP - INTERVAL '1 day');

UPDATE datetime_type
SET v1 = '2024-02-08', v2 = '16:30:00', v3 = CURRENT_TIMESTAMP, v4 = CURRENT_TIMESTAMP where id=4;

DELETE FROM datetime_type WHERE EXTRACT(YEAR FROM v1) < 2000;

INSERT INTO datetime_type2(v1)
VALUES ('2024-02-07 12:00:00');

DELETE FROM datetime_type2 WHERE v1 = '2024-02-07 12:00:00';

INSERT INTO datatypes2(v1, v2, v3, v4, v5)
VALUES ('{"key": "value"}', B'1010101010', ARRAY[1, 2, 3, 4], '{{"a", "b"}, {"c", "d"}}', B'001010101');

INSERT INTO datatypes2(v1, v2, v3, v4, v5)
VALUES ('{"key": "value"}', B'0000101010', ARRAY[1, 2, 3, 4], '{{"a", "b"}, {"c", "d"}}', B'1010101010101010101010');

UPDATE datatypes2
SET v1 = '{"updated": true}', v2 = B'0101010101', v5 = B'101010101010101010101010101010', v3 = ARRAY[5, 6, 7, 8], v4 = '{{"e", "f"}, {"g", "h"}}'
WHERE v1 IS NULL;

UPDATE hstore_example 
SET data = data || 'key3 => value3'
WHERE id = 1;

UPDATE hstore_example 
SET data = hstore('{"key1=value1, key2=value2"}', '{"key1=value1, key2={"key1=value1, key2=value2"}"}')
WHERE id = 3;

UPDATE hstore_example 
SET data = '"{\"key1=value1, key2=value2\"}"=>"{\"key1=value1, key2={\"key1=value1, key2=value2\"}\"}"'
WHERE id = 7;

INSERT INTO hstore_example (data) 
VALUES 
    ('key5 => value5, key6 => value6');

INSERT INTO hstore_example (data) 
VALUES 
    (hstore('{"key1=value1, key2=value2"}', '{"key1=value1, key2={"key1=value1, key2=value2"}"}'));
