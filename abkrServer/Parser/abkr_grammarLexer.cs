//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     ANTLR Version: 4.12.0
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

// Generated from abkr_grammarLexer.g4 by ANTLR 4.12.0

// Unreachable code detected
#pragma warning disable 0162
// The variable '...' is assigned but its value is never used
#pragma warning disable 0219
// Missing XML comment for publicly visible type or member '...'
#pragma warning disable 1591
// Ambiguous reference in cref attribute
#pragma warning disable 419

using System;
using System.IO;
using System.Text;
using Antlr4.Runtime;
using Antlr4.Runtime.Atn;
using Antlr4.Runtime.Misc;
using DFA = Antlr4.Runtime.Dfa.DFA;

[System.CodeDom.Compiler.GeneratedCode("ANTLR", "4.12.0")]
[System.CLSCompliant(false)]
public partial class abkr_grammarLexer : Lexer {
	protected static DFA[] decisionToDFA;
	protected static PredictionContextCache sharedContextCache = new PredictionContextCache();
	public const int
		CREATE=1, DATABASE=2, TABLE=3, INDEX=4, UNIQUE=5, DROP=6, ON=7, INT=8, 
		VARCHAR=9, PRIMARY=10, KEY=11, FOREIGN=12, REFERENCES=13, INSERT=14, INTO=15, 
		VALUES=16, DELETE=17, FROM=18, WHERE=19, SELECT=20, AND=21, ASTERISK=22, 
		GREATER_THAN=23, GREATER_EQUALS=24, LESS_THAN=25, LESS_EQUALS=26, DOT=27, 
		COMMA=28, LPAREN=29, RPAREN=30, EQUALS=31, IDENTIFIER=32, NUMBER=33, STRING=34, 
		WS=35;
	public static string[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static string[] modeNames = {
		"DEFAULT_MODE"
	};

	public static readonly string[] ruleNames = {
		"CREATE", "DATABASE", "TABLE", "INDEX", "UNIQUE", "DROP", "ON", "INT", 
		"VARCHAR", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "INSERT", "INTO", 
		"VALUES", "DELETE", "FROM", "WHERE", "SELECT", "AND", "ASTERISK", "GREATER_THAN", 
		"GREATER_EQUALS", "LESS_THAN", "LESS_EQUALS", "A", "B", "C", "D", "E", 
		"F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", 
		"T", "U", "V", "W", "X", "Y", "Z", "DOT", "COMMA", "LPAREN", "RPAREN", 
		"EQUALS", "IDENTIFIER", "NUMBER", "STRING", "WS"
	};


	public abkr_grammarLexer(ICharStream input)
	: this(input, Console.Out, Console.Error) { }

	public abkr_grammarLexer(ICharStream input, TextWriter output, TextWriter errorOutput)
	: base(input, output, errorOutput)
	{
		Interpreter = new LexerATNSimulator(this, _ATN, decisionToDFA, sharedContextCache);
	}

	private static readonly string[] _LiteralNames = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, "'*'", "'>'", 
		"'>='", "'<'", "'<='", "'.'", "','", "'('", "')'", "'='"
	};
	private static readonly string[] _SymbolicNames = {
		null, "CREATE", "DATABASE", "TABLE", "INDEX", "UNIQUE", "DROP", "ON", 
		"INT", "VARCHAR", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "INSERT", 
		"INTO", "VALUES", "DELETE", "FROM", "WHERE", "SELECT", "AND", "ASTERISK", 
		"GREATER_THAN", "GREATER_EQUALS", "LESS_THAN", "LESS_EQUALS", "DOT", "COMMA", 
		"LPAREN", "RPAREN", "EQUALS", "IDENTIFIER", "NUMBER", "STRING", "WS"
	};
	public static readonly IVocabulary DefaultVocabulary = new Vocabulary(_LiteralNames, _SymbolicNames);

	[NotNull]
	public override IVocabulary Vocabulary
	{
		get
		{
			return DefaultVocabulary;
		}
	}

	public override string GrammarFileName { get { return "abkr_grammarLexer.g4"; } }

	public override string[] RuleNames { get { return ruleNames; } }

	public override string[] ChannelNames { get { return channelNames; } }

	public override string[] ModeNames { get { return modeNames; } }

	public override int[] SerializedAtn { get { return _serializedATN; } }

	static abkr_grammarLexer() {
		decisionToDFA = new DFA[_ATN.NumberOfDecisions];
		for (int i = 0; i < _ATN.NumberOfDecisions; i++) {
			decisionToDFA[i] = new DFA(_ATN.GetDecisionState(i), i);
		}
	}
	private static int[] _serializedATN = {
		4,0,35,363,6,-1,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,
		6,2,7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,2,14,
		7,14,2,15,7,15,2,16,7,16,2,17,7,17,2,18,7,18,2,19,7,19,2,20,7,20,2,21,
		7,21,2,22,7,22,2,23,7,23,2,24,7,24,2,25,7,25,2,26,7,26,2,27,7,27,2,28,
		7,28,2,29,7,29,2,30,7,30,2,31,7,31,2,32,7,32,2,33,7,33,2,34,7,34,2,35,
		7,35,2,36,7,36,2,37,7,37,2,38,7,38,2,39,7,39,2,40,7,40,2,41,7,41,2,42,
		7,42,2,43,7,43,2,44,7,44,2,45,7,45,2,46,7,46,2,47,7,47,2,48,7,48,2,49,
		7,49,2,50,7,50,2,51,7,51,2,52,7,52,2,53,7,53,2,54,7,54,2,55,7,55,2,56,
		7,56,2,57,7,57,2,58,7,58,2,59,7,59,2,60,7,60,1,0,1,0,1,0,1,0,1,0,1,0,1,
		0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,2,1,2,1,2,1,2,1,2,1,3,1,3,
		1,3,1,3,1,3,1,3,1,4,1,4,1,4,1,4,1,4,1,4,1,4,1,5,1,5,1,5,1,5,1,5,1,6,1,
		6,1,6,1,7,1,7,1,7,1,7,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,8,1,9,1,9,1,9,1,9,
		1,9,1,9,1,9,1,9,1,10,1,10,1,10,1,10,1,11,1,11,1,11,1,11,1,11,1,11,1,11,
		1,11,1,12,1,12,1,12,1,12,1,12,1,12,1,12,1,12,1,12,1,12,1,12,1,13,1,13,
		1,13,1,13,1,13,1,13,1,13,1,14,1,14,1,14,1,14,1,14,1,15,1,15,1,15,1,15,
		1,15,1,15,1,15,1,16,1,16,1,16,1,16,1,16,1,16,1,16,1,17,1,17,1,17,1,17,
		1,17,1,18,1,18,1,18,1,18,1,18,1,18,1,19,1,19,1,19,1,19,1,19,1,19,1,19,
		1,20,1,20,1,20,1,20,1,21,1,21,1,22,1,22,1,23,1,23,1,23,1,24,1,24,1,25,
		1,25,1,25,1,26,1,26,1,27,1,27,1,28,1,28,1,29,1,29,1,30,1,30,1,31,1,31,
		1,32,1,32,1,33,1,33,1,34,1,34,1,35,1,35,1,36,1,36,1,37,1,37,1,38,1,38,
		1,39,1,39,1,40,1,40,1,41,1,41,1,42,1,42,1,43,1,43,1,44,1,44,1,45,1,45,
		1,46,1,46,1,47,1,47,1,48,1,48,1,49,1,49,1,50,1,50,1,51,1,51,1,52,1,52,
		1,53,1,53,1,54,1,54,1,55,1,55,1,56,1,56,1,57,1,57,5,57,334,8,57,10,57,
		12,57,337,9,57,1,58,4,58,340,8,58,11,58,12,58,341,1,59,1,59,1,59,1,59,
		1,59,1,59,5,59,350,8,59,10,59,12,59,353,9,59,1,59,1,59,1,60,4,60,358,8,
		60,11,60,12,60,359,1,60,1,60,0,0,61,1,1,3,2,5,3,7,4,9,5,11,6,13,7,15,8,
		17,9,19,10,21,11,23,12,25,13,27,14,29,15,31,16,33,17,35,18,37,19,39,20,
		41,21,43,22,45,23,47,24,49,25,51,26,53,0,55,0,57,0,59,0,61,0,63,0,65,0,
		67,0,69,0,71,0,73,0,75,0,77,0,79,0,81,0,83,0,85,0,87,0,89,0,91,0,93,0,
		95,0,97,0,99,0,101,0,103,0,105,27,107,28,109,29,111,30,113,31,115,32,117,
		33,119,34,121,35,1,0,31,2,0,65,65,97,97,2,0,66,66,98,98,2,0,67,67,99,99,
		2,0,68,68,100,100,2,0,69,69,101,101,2,0,70,70,102,102,2,0,71,71,103,103,
		2,0,72,72,104,104,2,0,73,73,105,105,2,0,74,74,106,106,2,0,75,75,107,107,
		2,0,76,76,108,108,2,0,77,77,109,109,2,0,78,78,110,110,2,0,79,79,111,111,
		2,0,80,80,112,112,2,0,81,81,113,113,2,0,82,82,114,114,2,0,83,83,115,115,
		2,0,84,84,116,116,2,0,85,85,117,117,2,0,86,86,118,118,2,0,87,87,119,119,
		2,0,88,88,120,120,2,0,89,89,121,121,2,0,90,90,122,122,2,0,65,90,97,122,
		4,0,48,57,65,90,95,95,97,122,1,0,48,57,1,0,39,39,3,0,9,10,13,13,32,32,
		342,0,1,1,0,0,0,0,3,1,0,0,0,0,5,1,0,0,0,0,7,1,0,0,0,0,9,1,0,0,0,0,11,1,
		0,0,0,0,13,1,0,0,0,0,15,1,0,0,0,0,17,1,0,0,0,0,19,1,0,0,0,0,21,1,0,0,0,
		0,23,1,0,0,0,0,25,1,0,0,0,0,27,1,0,0,0,0,29,1,0,0,0,0,31,1,0,0,0,0,33,
		1,0,0,0,0,35,1,0,0,0,0,37,1,0,0,0,0,39,1,0,0,0,0,41,1,0,0,0,0,43,1,0,0,
		0,0,45,1,0,0,0,0,47,1,0,0,0,0,49,1,0,0,0,0,51,1,0,0,0,0,105,1,0,0,0,0,
		107,1,0,0,0,0,109,1,0,0,0,0,111,1,0,0,0,0,113,1,0,0,0,0,115,1,0,0,0,0,
		117,1,0,0,0,0,119,1,0,0,0,0,121,1,0,0,0,1,123,1,0,0,0,3,130,1,0,0,0,5,
		139,1,0,0,0,7,145,1,0,0,0,9,151,1,0,0,0,11,158,1,0,0,0,13,163,1,0,0,0,
		15,166,1,0,0,0,17,170,1,0,0,0,19,178,1,0,0,0,21,186,1,0,0,0,23,190,1,0,
		0,0,25,198,1,0,0,0,27,209,1,0,0,0,29,216,1,0,0,0,31,221,1,0,0,0,33,228,
		1,0,0,0,35,235,1,0,0,0,37,240,1,0,0,0,39,246,1,0,0,0,41,253,1,0,0,0,43,
		257,1,0,0,0,45,259,1,0,0,0,47,261,1,0,0,0,49,264,1,0,0,0,51,266,1,0,0,
		0,53,269,1,0,0,0,55,271,1,0,0,0,57,273,1,0,0,0,59,275,1,0,0,0,61,277,1,
		0,0,0,63,279,1,0,0,0,65,281,1,0,0,0,67,283,1,0,0,0,69,285,1,0,0,0,71,287,
		1,0,0,0,73,289,1,0,0,0,75,291,1,0,0,0,77,293,1,0,0,0,79,295,1,0,0,0,81,
		297,1,0,0,0,83,299,1,0,0,0,85,301,1,0,0,0,87,303,1,0,0,0,89,305,1,0,0,
		0,91,307,1,0,0,0,93,309,1,0,0,0,95,311,1,0,0,0,97,313,1,0,0,0,99,315,1,
		0,0,0,101,317,1,0,0,0,103,319,1,0,0,0,105,321,1,0,0,0,107,323,1,0,0,0,
		109,325,1,0,0,0,111,327,1,0,0,0,113,329,1,0,0,0,115,331,1,0,0,0,117,339,
		1,0,0,0,119,343,1,0,0,0,121,357,1,0,0,0,123,124,3,57,28,0,124,125,3,87,
		43,0,125,126,3,61,30,0,126,127,3,53,26,0,127,128,3,91,45,0,128,129,3,61,
		30,0,129,2,1,0,0,0,130,131,3,59,29,0,131,132,3,53,26,0,132,133,3,91,45,
		0,133,134,3,53,26,0,134,135,3,55,27,0,135,136,3,53,26,0,136,137,3,89,44,
		0,137,138,3,61,30,0,138,4,1,0,0,0,139,140,3,91,45,0,140,141,3,53,26,0,
		141,142,3,55,27,0,142,143,3,75,37,0,143,144,3,61,30,0,144,6,1,0,0,0,145,
		146,3,69,34,0,146,147,3,79,39,0,147,148,3,59,29,0,148,149,3,61,30,0,149,
		150,3,99,49,0,150,8,1,0,0,0,151,152,3,93,46,0,152,153,3,79,39,0,153,154,
		3,69,34,0,154,155,3,85,42,0,155,156,3,93,46,0,156,157,3,61,30,0,157,10,
		1,0,0,0,158,159,3,59,29,0,159,160,3,87,43,0,160,161,3,81,40,0,161,162,
		3,83,41,0,162,12,1,0,0,0,163,164,3,81,40,0,164,165,3,79,39,0,165,14,1,
		0,0,0,166,167,3,69,34,0,167,168,3,79,39,0,168,169,3,91,45,0,169,16,1,0,
		0,0,170,171,3,95,47,0,171,172,3,53,26,0,172,173,3,87,43,0,173,174,3,57,
		28,0,174,175,3,67,33,0,175,176,3,53,26,0,176,177,3,87,43,0,177,18,1,0,
		0,0,178,179,3,83,41,0,179,180,3,87,43,0,180,181,3,69,34,0,181,182,3,77,
		38,0,182,183,3,53,26,0,183,184,3,87,43,0,184,185,3,101,50,0,185,20,1,0,
		0,0,186,187,3,73,36,0,187,188,3,61,30,0,188,189,3,101,50,0,189,22,1,0,
		0,0,190,191,3,63,31,0,191,192,3,81,40,0,192,193,3,87,43,0,193,194,3,61,
		30,0,194,195,3,69,34,0,195,196,3,65,32,0,196,197,3,79,39,0,197,24,1,0,
		0,0,198,199,3,87,43,0,199,200,3,61,30,0,200,201,3,63,31,0,201,202,3,61,
		30,0,202,203,3,87,43,0,203,204,3,61,30,0,204,205,3,79,39,0,205,206,3,57,
		28,0,206,207,3,61,30,0,207,208,3,89,44,0,208,26,1,0,0,0,209,210,3,69,34,
		0,210,211,3,79,39,0,211,212,3,89,44,0,212,213,3,61,30,0,213,214,3,87,43,
		0,214,215,3,91,45,0,215,28,1,0,0,0,216,217,3,69,34,0,217,218,3,79,39,0,
		218,219,3,91,45,0,219,220,3,81,40,0,220,30,1,0,0,0,221,222,3,95,47,0,222,
		223,3,53,26,0,223,224,3,75,37,0,224,225,3,93,46,0,225,226,3,61,30,0,226,
		227,3,89,44,0,227,32,1,0,0,0,228,229,3,59,29,0,229,230,3,61,30,0,230,231,
		3,75,37,0,231,232,3,61,30,0,232,233,3,91,45,0,233,234,3,61,30,0,234,34,
		1,0,0,0,235,236,3,63,31,0,236,237,3,87,43,0,237,238,3,81,40,0,238,239,
		3,77,38,0,239,36,1,0,0,0,240,241,3,97,48,0,241,242,3,67,33,0,242,243,3,
		61,30,0,243,244,3,87,43,0,244,245,3,61,30,0,245,38,1,0,0,0,246,247,3,89,
		44,0,247,248,3,61,30,0,248,249,3,75,37,0,249,250,3,61,30,0,250,251,3,57,
		28,0,251,252,3,91,45,0,252,40,1,0,0,0,253,254,3,53,26,0,254,255,3,79,39,
		0,255,256,3,59,29,0,256,42,1,0,0,0,257,258,5,42,0,0,258,44,1,0,0,0,259,
		260,5,62,0,0,260,46,1,0,0,0,261,262,5,62,0,0,262,263,5,61,0,0,263,48,1,
		0,0,0,264,265,5,60,0,0,265,50,1,0,0,0,266,267,5,60,0,0,267,268,5,61,0,
		0,268,52,1,0,0,0,269,270,7,0,0,0,270,54,1,0,0,0,271,272,7,1,0,0,272,56,
		1,0,0,0,273,274,7,2,0,0,274,58,1,0,0,0,275,276,7,3,0,0,276,60,1,0,0,0,
		277,278,7,4,0,0,278,62,1,0,0,0,279,280,7,5,0,0,280,64,1,0,0,0,281,282,
		7,6,0,0,282,66,1,0,0,0,283,284,7,7,0,0,284,68,1,0,0,0,285,286,7,8,0,0,
		286,70,1,0,0,0,287,288,7,9,0,0,288,72,1,0,0,0,289,290,7,10,0,0,290,74,
		1,0,0,0,291,292,7,11,0,0,292,76,1,0,0,0,293,294,7,12,0,0,294,78,1,0,0,
		0,295,296,7,13,0,0,296,80,1,0,0,0,297,298,7,14,0,0,298,82,1,0,0,0,299,
		300,7,15,0,0,300,84,1,0,0,0,301,302,7,16,0,0,302,86,1,0,0,0,303,304,7,
		17,0,0,304,88,1,0,0,0,305,306,7,18,0,0,306,90,1,0,0,0,307,308,7,19,0,0,
		308,92,1,0,0,0,309,310,7,20,0,0,310,94,1,0,0,0,311,312,7,21,0,0,312,96,
		1,0,0,0,313,314,7,22,0,0,314,98,1,0,0,0,315,316,7,23,0,0,316,100,1,0,0,
		0,317,318,7,24,0,0,318,102,1,0,0,0,319,320,7,25,0,0,320,104,1,0,0,0,321,
		322,5,46,0,0,322,106,1,0,0,0,323,324,5,44,0,0,324,108,1,0,0,0,325,326,
		5,40,0,0,326,110,1,0,0,0,327,328,5,41,0,0,328,112,1,0,0,0,329,330,5,61,
		0,0,330,114,1,0,0,0,331,335,7,26,0,0,332,334,7,27,0,0,333,332,1,0,0,0,
		334,337,1,0,0,0,335,333,1,0,0,0,335,336,1,0,0,0,336,116,1,0,0,0,337,335,
		1,0,0,0,338,340,7,28,0,0,339,338,1,0,0,0,340,341,1,0,0,0,341,339,1,0,0,
		0,341,342,1,0,0,0,342,118,1,0,0,0,343,351,5,39,0,0,344,350,8,29,0,0,345,
		346,5,39,0,0,346,350,5,39,0,0,347,348,5,92,0,0,348,350,5,39,0,0,349,344,
		1,0,0,0,349,345,1,0,0,0,349,347,1,0,0,0,350,353,1,0,0,0,351,349,1,0,0,
		0,351,352,1,0,0,0,352,354,1,0,0,0,353,351,1,0,0,0,354,355,5,39,0,0,355,
		120,1,0,0,0,356,358,7,30,0,0,357,356,1,0,0,0,358,359,1,0,0,0,359,357,1,
		0,0,0,359,360,1,0,0,0,360,361,1,0,0,0,361,362,6,60,0,0,362,122,1,0,0,0,
		6,0,335,341,349,351,359,1,6,0,0
	};

	public static readonly ATN _ATN =
		new ATNDeserializer().Deserialize(_serializedATN);


}
