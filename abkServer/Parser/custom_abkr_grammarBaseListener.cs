using System;
using Antlr4.Runtime.Misc;

namespace abkr.grammarParser
{
    public class CustomAbkrGrammarBaseListener : abkr_grammarBaseListener
    {
        public override void EnterStatement(abkr_grammarParser.StatementContext context)
        {
            Console.WriteLine("Entering statement");
            base.EnterStatement(context);
        }

        public override void ExitStatement(abkr_grammarParser.StatementContext context)
        {
            Console.WriteLine("Exiting statement");
            base.ExitStatement(context);
        }

        public override void EnterCreate_database_statement(abkr_grammarParser.Create_database_statementContext context)
        {
            Console.WriteLine($"Entering create_database_statement: {context.GetText()}");
            base.EnterCreate_database_statement(context);
        }

        public override void ExitCreate_database_statement(abkr_grammarParser.Create_database_statementContext context)
        {
            Console.WriteLine($"Exiting create_database_statement: {context.GetText()}");
            base.ExitCreate_database_statement(context);
        }

        public override void EnterCreate_table_statement(abkr_grammarParser.Create_table_statementContext context)
        {
            Console.WriteLine($"Entering create_table_statement: {context.GetText()}");
            base.EnterCreate_table_statement(context);
        }

        public override void ExitCreate_table_statement(abkr_grammarParser.Create_table_statementContext context)
        {
            Console.WriteLine($"Exiting create_table_statement: {context.GetText()}");
            base.ExitCreate_table_statement(context);
        }

        public override void EnterColumn_definition(abkr_grammarParser.Column_definitionContext context)
        {
            Console.WriteLine($"Entering column_definition: {context.GetText()}");
            base.EnterColumn_definition(context);
        }

        public override void ExitColumn_definition(abkr_grammarParser.Column_definitionContext context)
        {
            Console.WriteLine($"Exiting column_definition: {context.GetText()}");
            base.ExitColumn_definition(context);
        }
    }
}