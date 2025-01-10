INSERT INTO num_types(v1, v2, v3, v4, v5, v6)
VALUES (-32768, -214748364, -92233720368547758, -987.654, -98765.4321, -987654.321);

INSERT INTO num_types(v1, v2, v3, v4, v5, v6)
VALUES (0, null, null, null, null, null);

UPDATE num_types
SET v1 = v1 + 42, v2 = v2 - 42, v3 = v3 + 42, v4 = v4 + 1, v5 = v5 + 42, v6 = v6 - 42::money;

DELETE FROM num_types WHERE v5 < 0;

insert into decimal_types values(200, 790809990636198497784302463464676743730460045716056588284283619572097798777544920701390228264293554.869040822, 55613803484640647.03);
insert into decimal_types values(-3, 639331592204741887223305479788137535291488800417414936651322061138931510763125571702251187791371846.884254188, 99999999999999999.99);

UPDATE decimal_types
SET n1 = n1 * 0.2, n2 = n2 / 2;

DELETE FROM decimal_types WHERE id <= 0;

INSERT INTO datatypes1(bool_type, char_type1, varchar_type, byte_type, enum_type)
VALUES (FALSE, 'B', 'Goodbye, universe!', E'\\xABCD1234', 'Fri');

INSERT INTO datatypes1(bool_type, char_type1, varchar_type, byte_type, enum_type)
VALUES (TRUE, 'C', 'Random text', E'\\x00112233', 'Wed');

UPDATE datatypes1
SET bool_type = TRUE, char_type1 = 'Y', varchar_type = 'Modified!', enum_type = 'Tue' where enum_type = 'Mon';

UPDATE datatypes1
SET bool_type = NOT bool_type;

DELETE FROM datatypes1 WHERE bool_type = FALSE;

INSERT INTO datetime_type(v1, v2, v3, v4)
VALUES ('2024-04-22', '18:00:00', CURRENT_TIMESTAMP + INTERVAL '1 day', CURRENT_TIMESTAMP + INTERVAL '1 day');

UPDATE datetime_type
SET v1 = '2024-04-23', v2 = '20:30:00', v3 = CURRENT_TIMESTAMP - INTERVAL '1 month', v4 = CURRENT_TIMESTAMP - INTERVAL '1 month' where id > 4;

DELETE FROM datetime_type WHERE EXTRACT(HOUR FROM v2) > 12;

INSERT INTO datetime_type2(v1)
VALUES ('2024-04-22 09:15:00');

UPDATE datetime_type2
SET v1 = '2024-03-16 20:45:00' where id = 2;

INSERT INTO datatypes2(v1, v2, v3, v4,v5)
VALUES ('{"name": "John", "age": 30}', B'1100110011', ARRAY[5, 6, 7, 8], '{{"e", "f"}, {"g", "h"}}', B'00101010101010101010101010001010100101010101010101000');

INSERT INTO datatypes2(v1, v2, v3, v4, v5)
VALUES ('{"status": "active", "count": 42}', B'0101010101', ARRAY[9, 10, 11, 12], '{{"i", "j"}, {"k", "l"}}', B'10101010101010101');
INSERT INTO datatypes2(v1, v2, v3, v4,v5)
VALUES ('{"status": "active", "count": 42}', B'1101010101', ARRAY[9, 10, 11, 12], '{{"i", "j"}, {"k", "l"}}', B'01001010101010101000101010');

UPDATE datatypes2
SET v1 = '{"modified": false}', v2 = B'0011001100', v3 = ARRAY[1, 2, 3, 4], v4 = null where id=4;

UPDATE datatypes2
SET v1 = '{"new": "data"}', v2 = B'1111000011', v5=B'00101010010101010101010101001', v3 = ARRAY[9, 10, 11, 12], v4 = '{{"i", "j"}, {"k", "l"}}' where id=4;

DELETE FROM datatypes2
WHERE 5 = ANY(v3);

-- INSERT INTO hstore_example (data) 
-- VALUES 
--     ('key7 => value7, key8 => value8');

-- UPDATE hstore_example 
-- SET data = delete(data, 'key2')
-- WHERE id = 8;

-- DELETE FROM hstore_example WHERE data ? 'key5';

-- INSERT INTO hstore_example (data) 
-- VALUES 
-- (hstore('"{""key1"":""value1"",""key2"":""value2""}"', '{"key1=value1, key2={"key1=value1, key2=value2"}"}'));

-- UPDATE hstore_example 
-- SET data = hstore('{"key1=value1, key2=value2"}', '{"key1=value1, key2={"key1=value1, key2=value2"}"}')
-- WHERE id = 15;

-- UPDATE hstore_example 
-- SET data = '"{\"key1=value1, key2=value2\"}"=>"{\"key1=value1, key2={\"key1=value1, key2=value2\"}\"}"'
-- WHERE id = 14;
