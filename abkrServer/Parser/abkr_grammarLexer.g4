lexer grammar abkr_grammarLexer;

CREATE: C R E A T E;
DATABASE: D A T A B A S E;
TABLE: T A B L E;
INDEX: I N D E X;
UNIQUE: U N I Q U E;
DROP: D R O P;
ON: O N;
INT: I N T;
VARCHAR: V A R C H A R;
PRIMARY: P R I M A R Y;
KEY: K E Y;
FOREIGN: F O R E I G N;
REFERENCES: R E F E R E N C E S;
INSERT: I N S E R T;
INTO: I N T O;
VALUES: V A L U E S;
DELETE: D E L E T E;
FROM: F R O M;
WHERE: W H E R E;
SELECT: S E L E C T;
AND: A N D;
ASTERISK: '*';
GREATER_THAN: '>';
GREATER_EQUALS: '>=';
LESS_THAN: '<';
LESS_EQUALS: '<=';
DIFFERS: '!=';
INNER: I N N E R;
JOIN: J O I N;

fragment A: 'A' | 'a';
fragment B: 'B' | 'b';
fragment C: 'C' | 'c';
fragment D: 'D' | 'd';
fragment E: 'E' | 'e';
fragment F: 'F' | 'f';
fragment G: 'G' | 'g';
fragment H: 'H' | 'h';
fragment I: 'I' | 'i';
fragment J: 'J' | 'j';
fragment K: 'K' | 'k';
fragment L: 'L' | 'l';
fragment M: 'M' | 'm';
fragment N: 'N' | 'n';
fragment O: 'O' | 'o';
fragment P: 'P' | 'p';
fragment Q: 'Q' | 'q';
fragment R: 'R' | 'r';
fragment S: 'S' | 's';
fragment T: 'T' | 't';
fragment U: 'U' | 'u';
fragment V: 'V' | 'v';
fragment W: 'W' | 'w';
fragment X: 'X' | 'x';
fragment Y: 'Y' | 'y';
fragment Z: 'Z' | 'z';

DOT: '.';
COMMA: ',';
COLON: ':';
LPAREN: '(';
RPAREN: ')';
EQUALS: '=';

IDENTIFIER: [a-zA-Z][a-zA-Z0-9_]*;
NUMBER: [0-9]+;
STRING: '\'' (~'\'' | '\'\'' | '\\\'')* '\'';
WS: [ \t\r\n]+ -> skip;

COMMENT
    :   '/*' .*? '*/' -> skip
    ;

LINE_COMMENT
    :   '//' .*? '\n' -> skip
    ;

SQL_LINE_COMMENT
    :   '--' .*? '\n' -> skip
    ;


