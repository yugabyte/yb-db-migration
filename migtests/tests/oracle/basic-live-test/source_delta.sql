insert into x values(5,6);
insert into x values(4,5);
update x set id2=10 where id=1;
delete from x where id=3;
update x set id2=20 where id=5;
insert into x values(6,5);

-- DISABLING THIS PART OF THE TEST [Case-Sensitivity handling required for Oracle]
-- -- DMLs which can possible cause conflicts on import side (DELETE-INSERT, UPDATE-INSERT, DELETE-UPDATE, UPDATE-UPDATE)

-- -- DELETE-INSERT CONFLICT
-- -- Delete a row (id=1) and then insert with the same unique key value (id=9)
-- DELETE FROM user_table WHERE id = 1;
-- INSERT INTO user_table (id, email) VALUES (9, 'user1@example.com');

-- -- UPDATE-INSERT CONFLICT
-- -- Update a row (id=2) and then insert with the same unique key value (id=10)
-- UPDATE user_table SET email = 'updated_user2@example.com' WHERE id = 2;
-- INSERT INTO user_table (id, email) VALUES (10, 'user2@example.com');

-- -- DELETE-UPDATE CONFLICT
-- -- Delete a row (id=3) and then update another row with the same unique key value (id=5)
-- DELETE FROM user_table WHERE id = 3;
-- UPDATE user_table SET email = 'user3@example.com' WHERE id = 5;

-- -- UPDATE-UPDATE CONFLICT
-- -- Update a row (id=4) and then update another row with the same unique key value (id=6)
-- UPDATE user_table SET email = 'updated_user4@example.com' WHERE id = 4;
-- UPDATE user_table SET email = 'user4@example.com' WHERE id = 6;