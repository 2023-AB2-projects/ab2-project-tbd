using System;
using Antlr4.Runtime;
using Antlr4.Runtime.Misc;
using Antlr4.Runtime.Tree;

namespace abkr.grammarParser
{
    public class CustomAbkrGrammarListener : Iabkr_grammarListener
    {
        public void EnterStatement(abkr_grammarParser.StatementContext context)
        {
            Console.WriteLine("Entering statement");
        }

        public void ExitStatement(abkr_grammarParser.StatementContext context)
        {
            Console.WriteLine("Exiting statement");
        }

        public void EnterCreate_database_statement(abkr_grammarParser.Create_database_statementContext context)
        {
            Console.WriteLine($"Entering create_database_statement: {context.GetText()}");
        }

        public void ExitCreate_database_statement(abkr_grammarParser.Create_database_statementContext context)
        {
            Console.WriteLine($"Exiting create_database_statement: {context.GetText()}");
        }

        public void EnterCreate_table_statement(abkr_grammarParser.Create_table_statementContext context)
        {
            Console.WriteLine($"Entering create_table_statement: {context.GetText()}");
        }

        public void ExitCreate_table_statement(abkr_grammarParser.Create_table_statementContext context)
        {
            Console.WriteLine($"Exiting create_table_statement: {context.GetText()}");
        }

        public void EnterColumn_definition(abkr_grammarParser.Column_definitionContext context)
        {
            Console.WriteLine($"Entering column_definition: {context.GetText()}");
        }

        public void ExitColumn_definition(abkr_grammarParser.Column_definitionContext context)
        {
            Console.WriteLine($"Exiting column_definition: {context.GetText()}");
        }

        public void EnterData_type(abkr_grammarParser.Data_typeContext context)
        {
            Console.WriteLine($"Entering data_type: {context.GetText()}");
        }

        public void ExitData_type(abkr_grammarParser.Data_typeContext context)
        {
            Console.WriteLine($"Exiting data_type: {context.GetText()}");
        }

        public void EnterIdentifier(abkr_grammarParser.IdentifierContext context)
        {
            Console.WriteLine($"Entering identifier: {context.GetText()}");
        }

        public void ExitIdentifier(abkr_grammarParser.IdentifierContext context)
        {
            Console.WriteLine($"Exiting identifier: {context.GetText()}");
        }

        // Implement the rest of the methods from the Iabkr_grammarListener interface
        public void EnterColumn_definition_list(abkr_grammarParser.Column_definition_listContext context)
        {
            // Add your implementation here
        }

        public void ExitColumn_definition_list(abkr_grammarParser.Column_definition_listContext context)
        {
            // Add your implementation here
        }

        public void EnterEveryRule(ParserRuleContext context) { }
        public void ExitEveryRule(ParserRuleContext context) { }
        public void VisitTerminal(ITerminalNode node) { }
        public void VisitErrorNode(IErrorNode node) { }
    }
}
