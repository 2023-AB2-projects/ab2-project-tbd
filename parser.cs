using System;
using System.Collections.Generic;
using MiniSQL.Library.Models;

namespace MiniSQL.Parser
{
    public class Parser
    {
        private Lexer _lexer;
        private Token _currentToken;

        public Parser(string input)
        {
            _lexer = new Lexer(input);
            _currentToken = _lexer.GetNextToken();
        }

        // helper methods to match tokens
        private void Match(TokenType expectedTokenType)
        {
            if (_currentToken.Type == expectedTokenType)
            {
                _currentToken = _lexer.GetNextToken();
            }
            else
            {
                throw new Exception($"Syntax error: Expected {expectedTokenType} but found {_currentToken.Type}");
            }
        }

        // parsing methods for each non-terminal symbol in the grammar
        private void ParseCreateDatabaseStatement()
        {
            // parse "create" token
            Match(TokenType.Create);

            // parse "database" token
            Match(TokenType.Database);

            // parse database name
            string databaseName = _currentToken.Value;
            Match(TokenType.Identifier);

            // create database with given name
            // TODO: implement database creation logic
        }

        private void ParseCreateTableStatement()
        {
            // parse "create" token
            Match(TokenType.Create);

            // parse "table" token
            Match(TokenType.Table);

            // parse table name
            string tableName = _currentToken.Value;
            Match(TokenType.Identifier);

            // parse column definitions
            List<ColumnDefinition> columns = ParseColumnDefinitions();

            // create table with given name and columns
            // TODO: implement table creation logic
        }

        private List<ColumnDefinition> ParseColumnDefinitions()
        {
            List<ColumnDefinition> columns = new List<ColumnDefinition>();

            // parse left parenthesis
            Match(TokenType.LeftParenthesis);

            // parse column definitions
            while (_currentToken.Type != TokenType.RightParenthesis)
            {
                // parse column name
                string columnName = _currentToken.Value;
                Match(TokenType.Identifier);

                // parse column type
                DataType dataType = ParseDataType();

                // add column to list
                columns.Add(new ColumnDefinition(columnName, dataType));

                // parse comma or right parenthesis
                if (_currentToken.Type == TokenType.Comma)
                {
                    Match(TokenType.Comma);
                }
                else if (_currentToken.Type != TokenType.RightParenthesis)
                {
                    throw new Exception($"Syntax error: Expected ',' or ')' but found {_currentToken.Type}");
                }
            }

            // parse right parenthesis
            Match(TokenType.RightParenthesis);

            return columns;
        }

        private DataType ParseDataType()
        {
            // parse data type
            if (_currentToken.Type == TokenType.Int)
            {
                Match(TokenType.Int);
                return DataType.Int;
            }
            else if (_currentToken.Type == TokenType.VarChar)
            {
                Match(TokenType.VarChar);
                // parse string length
                int length = int.Parse(_currentToken.Value);
                Match(TokenType.Number);
                return new VarCharDataType(length);
            }
            else
            {
                throw new Exception($"Syntax error: Expected data type but found {_currentToken.Type}");
            }
        }

        // top-level parsing method
        public string Parse()
        {
            // parse SQL statements until end of input
            while (_currentToken.Type != TokenType.EndOfFile)
            {
                if (_currentToken.Type == TokenType.Create)
                {
                    // parse create statement
                    _currentToken = _lexer.GetNextToken();
                    if (_currentToken.Type == TokenType.Database)
                    {
                        ParseCreateDatabaseStatement();
                        return "Database created successfully";
                    }
                    else if (_currentToken.Type == TokenType.Table)
                    {
                        ParseCreateTableStatement();
                        return "Table created successfully";
                    }
                    else
                    {
                        throw new Exception($"Syntax error: Expected 'database' or 'table' but found {_currentToken.Type}");
                    }
                }
                else
                {
                    throw new Exception($"Syntax error: Expected 'create' but found {_currentToken.Type}");
                }
            }
            return "Parsing completed";
        }
    }
}

