CREATE KEYSPACE flink_db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE flink_db;
CREATE TABLE users(profile_id int PRIMARY KEY, user_name text, message text, domain list);

INSERT INTO users (profile_id, user_name, message, domain) VALUES( 102103, 'Prashant', 'TestPrashant','Maths');
INSERT INTO users (profile_id, user_name, message, domain) VALUES( 102104, 'Alisha', 'TestAlisha','English');
INSERT INTO users (profile_id, user_name, message, domain) VALUES( 102105, 'Abdul', 'TestAbdul','Maths');

INSERT INTO users (profile_id, user_name, message, domain) VALUES();

CREATE TABLE teachers (
    teacher_id int PRIMARY KEY,
    teacher_name text,
    domains list<text>
);

DELETE FROM teachers WHERE teacher_id = 2;


INSERT INTO teachers (teacher_id, teacher_name, domains)
VALUES (101, 'John', ['Math', 'Physics', 'Chemistry']);

INSERT INTO teachers (teacher_id, teacher_name, domains)
VALUES (102, 'Smith', ['Chemistry', 'Math']);

INSERT INTO teachers (teacher_id, teacher_name, domains)
VALUES (103, 'Bob', ['Physics', 'Math', 'English']);




