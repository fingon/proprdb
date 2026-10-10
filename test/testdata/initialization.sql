CREATE TEMP TABLE initialization_objects AS
WITH RECURSIVE sequence(n) AS (
    SELECT 1 WHERE ? > 0
    UNION ALL SELECT n + 1 FROM sequence WHERE n < ?
)
SELECT printf('01951d6e-a000-7000-8000-%012x', n) AS id FROM sequence;
INSERT INTO generatedtest_example_person (id, at_ns, data, name, age)
SELECT id, 1, X'0a034164611025', 'Ada', 37 FROM initialization_objects;
INSERT INTO _deleted (table_name, id, at_ns)
SELECT 'unbound', id, 1 FROM initialization_objects;
INSERT INTO _sync (object_id, table_name, at_ns, remote)
SELECT id, 'unbound', 1, 'remote' FROM initialization_objects;
INSERT INTO _unknown_types (type_name, id, at_ns, deleted, data_json)
SELECT 'unbound', id, 1, 0, '{"@type":"type.googleapis.com/unbound"}' FROM initialization_objects;
INSERT INTO _unknown_sync (type_name, id, at_ns, remote)
SELECT 'unbound', id, 1, 'remote' FROM initialization_objects;
INSERT INTO _export_batches (batch_id, database_id, remote)
VALUES ('benchmark', 'benchmark', 'remote');
INSERT INTO _export_batch_entries (batch_id, sequence, table_name, object_id, at_ns)
SELECT 'benchmark', rowid, 'unbound', id, 1 FROM initialization_objects;
DROP TABLE initialization_objects;
