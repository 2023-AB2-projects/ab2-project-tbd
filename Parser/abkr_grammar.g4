grammar abkr_grammar;

statement: create_database_statement | create_table_statement | drop_database_statement | drop_table_statement | create_index_statement | drop_index_statement;

create_database_statement: CREATE DATABASE identifier;

create_table_statement: CREATE TABLE identifier '.' identifier '(' column_definition_list ')';

drop_database_statement: DROP DATABASE identifier;

drop_table_statement: DROP TABLE identifier '.' identifier;

create_index_statement: CREATE INDEX identifier ON identifier '(' identifier_list ')';

drop_index_statement: DROP INDEX identifier ON identifier;

column_definition_list: column_definition (',' column_definition)*;

column_definition: identifier data_type;

identifier_list: identifier (',' identifier)*;

data_type: INT | VARCHAR '(' NUMBER ')';

identifier: IDENTIFIER;

CREATE: 'CREATE';
DATABASE: 'DATABASE';
TABLE: 'TABLE';
INDEX: 'INDEX';
DROP: 'DROP';
ON: 'ON';
INT: 'INT';
VARCHAR: 'VARCHAR';
NUMBER: [0-9]+;
IDENTIFIER: [a-zA-Z][a-zA-Z0-9_]*;
WS: [ \t\r\n]+ -> skip;
