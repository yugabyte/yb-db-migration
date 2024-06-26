INSERT INTO test_simple (id, val) VALUES (2, 'Second Value');
INSERT INTO test_simple (id, val) VALUES (4, 'Fourth Value');
UPDATE test_simple SET val = 'Updated Second Value' WHERE id = 2;
DELETE FROM test_simple WHERE id = 2;
