//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     ANTLR Version: 4.6
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

// Generated from abkr_grammar.g4 by ANTLR 4.6

// Unreachable code detected
#pragma warning disable 0162
// The variable '...' is assigned but its value is never used
#pragma warning disable 0219
// Missing XML comment for publicly visible type or member '...'
#pragma warning disable 1591
// Ambiguous reference in cref attribute
#pragma warning disable 419

using System;
using System.Text;
using Antlr4.Runtime;
using Antlr4.Runtime.Atn;
using Antlr4.Runtime.Misc;
using DFA = Antlr4.Runtime.Dfa.DFA;

namespace abkr.grammarParser
{
	[System.CodeDom.Compiler.GeneratedCode("ANTLR", "4.6")]
	[System.CLSCompliant(false)]
	public partial class abkr_grammarLexer : Lexer
	{
		protected static DFA[] decisionToDFA;
		protected static PredictionContextCache sharedContextCache = new PredictionContextCache();
		public const int
			T__0 = 1, T__1 = 2, T__2 = 3, CREATE = 4, DATABASE = 5, TABLE = 6, INT = 7, VARCHAR = 8,
			NUMBER = 9, IDENTIFIER = 10, WS = 11;
		public static string[] modeNames = {
		"DEFAULT_MODE"
	};

		public static readonly string[] ruleNames = {
		"T__0", "T__1", "T__2", "CREATE", "DATABASE", "TABLE", "INT", "VARCHAR",
		"NUMBER", "IDENTIFIER", "WS"
	};


		public abkr_grammarLexer(ICharStream input)
			: base(input)
		{
			Interpreter = new LexerATNSimulator(this, _ATN);
		}

		private static readonly string[] _LiteralNames = {
		null, "'('", "')'", "','", "'CREATE'", "'DATABASE'", "'TABLE'", "'INT'",
		"'VARCHAR'"
	};
		private static readonly string[] _SymbolicNames = {
		null, null, null, null, "CREATE", "DATABASE", "TABLE", "INT", "VARCHAR",
		"NUMBER", "IDENTIFIER", "WS"
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

		public override string GrammarFileName { get { return "abkr_grammar.g4"; } }

		public override string[] RuleNames { get { return ruleNames; } }

		public override string[] ModeNames { get { return modeNames; } }

		public override string SerializedAtn { get { return _serializedATN; } }

		static abkr_grammarLexer()
		{
			decisionToDFA = new DFA[_ATN.NumberOfDecisions];
			for (int i = 0; i < _ATN.NumberOfDecisions; i++)
			{
				decisionToDFA[i] = new DFA(_ATN.GetDecisionState(i), i);
			}
		}
		private static string _serializedATN = _serializeATN();
		private static string _serializeATN()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("\x3\x430\xD6D1\x8206\xAD2D\x4417\xAEF1\x8D80\xAADD\x2\rT");
			sb.Append("\b\x1\x4\x2\t\x2\x4\x3\t\x3\x4\x4\t\x4\x4\x5\t\x5\x4\x6\t\x6");
			sb.Append("\x4\a\t\a\x4\b\t\b\x4\t\t\t\x4\n\t\n\x4\v\t\v\x4\f\t\f\x3\x2");
			sb.Append("\x3\x2\x3\x3\x3\x3\x3\x4\x3\x4\x3\x5\x3\x5\x3\x5\x3\x5\x3\x5");
			sb.Append("\x3\x5\x3\x5\x3\x6\x3\x6\x3\x6\x3\x6\x3\x6\x3\x6\x3\x6\x3\x6");
			sb.Append("\x3\x6\x3\a\x3\a\x3\a\x3\a\x3\a\x3\a\x3\b\x3\b\x3\b\x3\b\x3");
			sb.Append("\t\x3\t\x3\t\x3\t\x3\t\x3\t\x3\t\x3\t\x3\n\x6\n\x43\n\n\r\n");
			sb.Append("\xE\n\x44\x3\v\x3\v\a\vI\n\v\f\v\xE\vL\v\v\x3\f\x6\fO\n\f\r");
			sb.Append("\f\xE\fP\x3\f\x3\f\x2\x2\r\x3\x3\x5\x4\a\x5\t\x6\v\a\r\b\xF");
			sb.Append("\t\x11\n\x13\v\x15\f\x17\r\x3\x2\x6\x3\x2\x32;\x4\x2\x43\\\x63");
			sb.Append("|\x6\x2\x32;\x43\\\x61\x61\x63|\x5\x2\v\f\xF\xF\"\"V\x2\x3\x3");
			sb.Append("\x2\x2\x2\x2\x5\x3\x2\x2\x2\x2\a\x3\x2\x2\x2\x2\t\x3\x2\x2\x2");
			sb.Append("\x2\v\x3\x2\x2\x2\x2\r\x3\x2\x2\x2\x2\xF\x3\x2\x2\x2\x2\x11");
			sb.Append("\x3\x2\x2\x2\x2\x13\x3\x2\x2\x2\x2\x15\x3\x2\x2\x2\x2\x17\x3");
			sb.Append("\x2\x2\x2\x3\x19\x3\x2\x2\x2\x5\x1B\x3\x2\x2\x2\a\x1D\x3\x2");
			sb.Append("\x2\x2\t\x1F\x3\x2\x2\x2\v&\x3\x2\x2\x2\r/\x3\x2\x2\x2\xF\x35");
			sb.Append("\x3\x2\x2\x2\x11\x39\x3\x2\x2\x2\x13\x42\x3\x2\x2\x2\x15\x46");
			sb.Append("\x3\x2\x2\x2\x17N\x3\x2\x2\x2\x19\x1A\a*\x2\x2\x1A\x4\x3\x2");
			sb.Append("\x2\x2\x1B\x1C\a+\x2\x2\x1C\x6\x3\x2\x2\x2\x1D\x1E\a.\x2\x2");
			sb.Append("\x1E\b\x3\x2\x2\x2\x1F \a\x45\x2\x2 !\aT\x2\x2!\"\aG\x2\x2\"");
			sb.Append("#\a\x43\x2\x2#$\aV\x2\x2$%\aG\x2\x2%\n\x3\x2\x2\x2&\'\a\x46");
			sb.Append("\x2\x2\'(\a\x43\x2\x2()\aV\x2\x2)*\a\x43\x2\x2*+\a\x44\x2\x2");
			sb.Append("+,\a\x43\x2\x2,-\aU\x2\x2-.\aG\x2\x2.\f\x3\x2\x2\x2/\x30\aV");
			sb.Append("\x2\x2\x30\x31\a\x43\x2\x2\x31\x32\a\x44\x2\x2\x32\x33\aN\x2");
			sb.Append("\x2\x33\x34\aG\x2\x2\x34\xE\x3\x2\x2\x2\x35\x36\aK\x2\x2\x36");
			sb.Append("\x37\aP\x2\x2\x37\x38\aV\x2\x2\x38\x10\x3\x2\x2\x2\x39:\aX\x2");
			sb.Append("\x2:;\a\x43\x2\x2;<\aT\x2\x2<=\a\x45\x2\x2=>\aJ\x2\x2>?\a\x43");
			sb.Append("\x2\x2?@\aT\x2\x2@\x12\x3\x2\x2\x2\x41\x43\t\x2\x2\x2\x42\x41");
			sb.Append("\x3\x2\x2\x2\x43\x44\x3\x2\x2\x2\x44\x42\x3\x2\x2\x2\x44\x45");
			sb.Append("\x3\x2\x2\x2\x45\x14\x3\x2\x2\x2\x46J\t\x3\x2\x2GI\t\x4\x2\x2");
			sb.Append("HG\x3\x2\x2\x2IL\x3\x2\x2\x2JH\x3\x2\x2\x2JK\x3\x2\x2\x2K\x16");
			sb.Append("\x3\x2\x2\x2LJ\x3\x2\x2\x2MO\t\x5\x2\x2NM\x3\x2\x2\x2OP\x3\x2");
			sb.Append("\x2\x2PN\x3\x2\x2\x2PQ\x3\x2\x2\x2QR\x3\x2\x2\x2RS\b\f\x2\x2");
			sb.Append("S\x18\x3\x2\x2\x2\x6\x2\x44JP\x3\b\x2\x2");
			return sb.ToString();
		}

		public static readonly ATN _ATN =
			new ATNDeserializer().Deserialize(_serializedATN.ToCharArray());


	}
}