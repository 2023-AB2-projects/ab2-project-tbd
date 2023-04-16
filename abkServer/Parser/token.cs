namespace abkrServer.Parser
{
    public enum TokenType
    {
        // Add your token types here, e.g.
        Create,
        Database,
        Table,
        Int,
        VarChar,
        Identifier,
        Number,
        Comma,
        LeftParenthesis,
        RightParenthesis,
        EndOfFile,
        //...
    }

    public class Token
    {
        public TokenType Type { get; set; }
        public string Value { get; set; }

        public Token(TokenType type, string value)
        {
            Type = type;
            Value = value;
        }
    }
}
