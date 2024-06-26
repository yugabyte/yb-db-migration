INSERT INTO test_simple (id, val) VALUES (2, 'Second Value');
INSERT INTO test_simple (id, val) VALUES (5, 'Fifth Value');
DELETE FROM test_simple WHERE id = 5;
UPDATE test_simple SET val = 'Updated Fourth Value', id=3 WHERE id = 2;
