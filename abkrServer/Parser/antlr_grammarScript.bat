@echo off
java -jar antlr-4.12.0-complete.jar -Dlanguage=CSharp abkr_grammarLexer.g4
java -jar antlr-4.12.0-complete.jar -Dlanguage=CSharp abkr_grammar.g4
pause
