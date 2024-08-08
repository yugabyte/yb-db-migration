SET ECHO ON

DECLARE
   v_deleted_objects BOOLEAN := TRUE;

   PROCEDURE delete_objects IS
   BEGIN
      FOR cur_rec IN (SELECT object_name, object_type
                        FROM user_objects
                       WHERE object_type IN
                                ('TABLE',
                                 'TYPE',
                                 'TABLE PARTITION',
                                 'VIEW',
                                 'PACKAGE',
                                 'PROCEDURE',
                                 'FUNCTION',
                                 'SEQUENCE',
                                 'SYNONYM',
                                 'INDEX',
                                 'MATERIALIZED VIEW',
                                 'CLUSTER',
                                 'INDEX PARTITION'
                                )) LOOP
         BEGIN
            IF cur_rec.object_type = 'TABLE' THEN
               EXECUTE IMMEDIATE 'DROP ' || cur_rec.object_type || ' "' || cur_rec.object_name || '" CASCADE CONSTRAINTS';
            ELSE
               EXECUTE IMMEDIATE 'DROP ' || cur_rec.object_type || ' "' || cur_rec.object_name || '"';
            END IF;
            v_deleted_objects := TRUE;
         EXCEPTION
            WHEN OTHERS THEN
               DBMS_OUTPUT.put_line('FAILED: DROP ' || cur_rec.object_type || ' "' || cur_rec.object_name || '"');
         END;
      END LOOP;
   END;

BEGIN
   -- Loop until no objects are deleted
   WHILE v_deleted_objects LOOP
      v_deleted_objects := FALSE;
      delete_objects;
   END LOOP;

   COMMIT;
END;
/

PURGE RECYCLEBIN;

SET ECHO OFF
