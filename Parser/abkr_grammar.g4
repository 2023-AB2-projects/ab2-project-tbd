grammar abkr_grammar;

statement: create_database_statement | create_table_statement;

create_database_statement: CREATE DATABASE identifier;

create_table_statement: CREATE TABLE identifier '(' column_definition_list ')';

column_definition_list: column_definition (',' column_definition)*;

column_definition: identifier data_type;

data_type: INT | VARCHAR '(' NUMBER ')';

identifier: IDENTIFIER;

CREATE: 'CREATE';
DATABASE: 'DATABASE';
TABLE: 'TABLE';
INT: 'INT';
VARCHAR: 'VARCHAR';
NUMBER: [0-9]+;
IDENTIFIER: [a-zA-Z][a-zA-Z0-9_]*;
WS: [ \t\r\n]+ -> skip;
