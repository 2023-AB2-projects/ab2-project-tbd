grammar abkr_grammar;

statement: create_database_statement | create_table_statement | drop_database_statement | drop_table_statement | create_index_statement | drop_index_statement | insert_statement | delete_statement;

create_database_statement: CREATE DATABASE identifier;

create_table_statement: CREATE TABLE identifier '.' identifier '(' column_definition_list ')';

drop_database_statement: DROP DATABASE identifier;

drop_table_statement: DROP TABLE identifier '.' identifier;

create_index_statement: CREATE (UNIQUE)? INDEX identifier ON identifier '(' identifier_list ')';

drop_index_statement: DROP INDEX identifier ON identifier;

column_definition_list: column_definition (',' column_definition)*;

column_definition: identifier data_type column_constraint*;

column_constraint
    : PRIMARY KEY
    | UNIQUE
    | FOREIGN KEY REFERENCES identifier '.' identifier
    ;

data_type: INT | VARCHAR '(' NUMBER ')';

identifier_list: identifier (',' identifier)*;

identifier: IDENTIFIER;

insert_statement: INSERT INTO identifier '.' identifier ( '(' identifier_list ')' )? VALUES '(' value_list ')';

delete_statement: DELETE FROM identifier '.' identifier WHERE identifier '=' value;

value_list: value (',' value)*;

value: STRING | NUMBER;

STRING: '\'' (~'\'' | '\'\'' | '\\\'')* '\'';

CREATE: 'CREATE';
DATABASE: 'DATABASE';
TABLE: 'TABLE';
INDEX: 'INDEX';
UNIQUE: 'UNIQUE';
DROP: 'DROP';
ON: 'ON';
INT: 'INT';
VARCHAR: 'VARCHAR';
PRIMARY: 'PRIMARY';
KEY: 'KEY';
FOREIGN: 'FOREIGN';
REFERENCES: 'REFERENCES';
INSERT: 'INSERT';
INTO: 'INTO';
VALUES: 'VALUES';
DELETE: 'DELETE';
FROM: 'FROM';
WHERE: 'WHERE';
NUMBER: [0-9]+;
IDENTIFIER: [a-zA-Z][a-zA-Z0-9_]*;
WS: [ \t\r\n]+ -> skip;
