// $ANTLR 3.5.2 Dot.g 2015-04-28 15:52:37

package org.cesta.parsers.dot;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class DotLexer extends Lexer {
	public static final int EOF=-1;
	public static final int A=4;
	public static final int ALPHACHAR=5;
	public static final int ATTR=6;
	public static final int ATTR_LIST=7;
	public static final int B=8;
	public static final int C=9;
	public static final int COLON=10;
	public static final int COMMA=11;
	public static final int COMMENT=12;
	public static final int C_BRACKET=13;
	public static final int C_SQR_BRACKET=14;
	public static final int D=15;
	public static final int DIGRAPH=16;
	public static final int E=17;
	public static final int EDGE=18;
	public static final int EDGEOP=19;
	public static final int EDGE_STMT=20;
	public static final int EQUAL=21;
	public static final int ESCAPE_SEQUENCE=22;
	public static final int F=23;
	public static final int G=24;
	public static final int GRAPH=25;
	public static final int GRAPH_ROOT=26;
	public static final int H=27;
	public static final int HTMLSTR=28;
	public static final int I=29;
	public static final int ID=30;
	public static final int J=31;
	public static final int K=32;
	public static final int L=33;
	public static final int LINE_COMMENT=34;
	public static final int LPAREN=35;
	public static final int M=36;
	public static final int N=37;
	public static final int NEWLINE=38;
	public static final int NODE=39;
	public static final int NODE_STMT=40;
	public static final int NUMBER=41;
	public static final int O=42;
	public static final int O_BRACKET=43;
	public static final int O_SQR_BRACKET=44;
	public static final int P=45;
	public static final int Q=46;
	public static final int QUOTEDSTR=47;
	public static final int R=48;
	public static final int RPAREN=49;
	public static final int S=50;
	public static final int SEMI_COLON=51;
	public static final int STMT_LIST=52;
	public static final int STR=53;
	public static final int STRICT=54;
	public static final int SUBGRAPH=55;
	public static final int SUBGRAPH_ROOT=56;
	public static final int T=57;
	public static final int U=58;
	public static final int V=59;
	public static final int VALIDSTR=60;
	public static final int W=61;
	public static final int WS=62;
	public static final int X=63;
	public static final int Y=64;
	public static final int Z=65;



	// delegates
	// delegators
	public Lexer[] getDelegates() {
		return new Lexer[] {};
	}

	public DotLexer() {} 
	public DotLexer(CharStream input) {
		this(input, new RecognizerSharedState());
	}
	public DotLexer(CharStream input, RecognizerSharedState state) {
		super(input,state);
	}
	@Override public String getGrammarFileName() { return "Dot.g"; }

	// $ANTLR start "COLON"
	public final void mCOLON() throws RecognitionException {
		try {
			int _type = COLON;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:8:7: ( ':' )
			// Dot.g:8:9: ':'
			{
			match(':'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "COLON"

	// $ANTLR start "COMMA"
	public final void mCOMMA() throws RecognitionException {
		try {
			int _type = COMMA;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:9:7: ( ',' )
			// Dot.g:9:9: ','
			{
			match(','); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "COMMA"

	// $ANTLR start "C_BRACKET"
	public final void mC_BRACKET() throws RecognitionException {
		try {
			int _type = C_BRACKET;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:10:11: ( '}' )
			// Dot.g:10:13: '}'
			{
			match('}'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "C_BRACKET"

	// $ANTLR start "C_SQR_BRACKET"
	public final void mC_SQR_BRACKET() throws RecognitionException {
		try {
			int _type = C_SQR_BRACKET;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:11:15: ( ']' )
			// Dot.g:11:17: ']'
			{
			match(']'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "C_SQR_BRACKET"

	// $ANTLR start "EQUAL"
	public final void mEQUAL() throws RecognitionException {
		try {
			int _type = EQUAL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:12:7: ( '=' )
			// Dot.g:12:9: '='
			{
			match('='); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "EQUAL"

	// $ANTLR start "LPAREN"
	public final void mLPAREN() throws RecognitionException {
		try {
			int _type = LPAREN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:13:8: ( '(' )
			// Dot.g:13:10: '('
			{
			match('('); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "LPAREN"

	// $ANTLR start "O_BRACKET"
	public final void mO_BRACKET() throws RecognitionException {
		try {
			int _type = O_BRACKET;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:14:11: ( '{' )
			// Dot.g:14:13: '{'
			{
			match('{'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "O_BRACKET"

	// $ANTLR start "O_SQR_BRACKET"
	public final void mO_SQR_BRACKET() throws RecognitionException {
		try {
			int _type = O_SQR_BRACKET;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:15:15: ( '[' )
			// Dot.g:15:17: '['
			{
			match('['); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "O_SQR_BRACKET"

	// $ANTLR start "RPAREN"
	public final void mRPAREN() throws RecognitionException {
		try {
			int _type = RPAREN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:16:8: ( ')' )
			// Dot.g:16:10: ')'
			{
			match(')'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "RPAREN"

	// $ANTLR start "SEMI_COLON"
	public final void mSEMI_COLON() throws RecognitionException {
		try {
			int _type = SEMI_COLON;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:17:12: ( ';' )
			// Dot.g:17:14: ';'
			{
			match(';'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "SEMI_COLON"

	// $ANTLR start "GRAPH"
	public final void mGRAPH() throws RecognitionException {
		try {
			int _type = GRAPH;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:201:6: ( G R A P H )
			// Dot.g:201:8: G R A P H
			{
			mG(); 

			mR(); 

			mA(); 

			mP(); 

			mH(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "GRAPH"

	// $ANTLR start "DIGRAPH"
	public final void mDIGRAPH() throws RecognitionException {
		try {
			int _type = DIGRAPH;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:202:8: ( D I G R A P H )
			// Dot.g:202:10: D I G R A P H
			{
			mD(); 

			mI(); 

			mG(); 

			mR(); 

			mA(); 

			mP(); 

			mH(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "DIGRAPH"

	// $ANTLR start "STRICT"
	public final void mSTRICT() throws RecognitionException {
		try {
			int _type = STRICT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:203:7: ( S T R I C T )
			// Dot.g:203:9: S T R I C T
			{
			mS(); 

			mT(); 

			mR(); 

			mI(); 

			mC(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "STRICT"

	// $ANTLR start "NODE"
	public final void mNODE() throws RecognitionException {
		try {
			int _type = NODE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:204:5: ( N O D E )
			// Dot.g:204:7: N O D E
			{
			mN(); 

			mO(); 

			mD(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "NODE"

	// $ANTLR start "EDGE"
	public final void mEDGE() throws RecognitionException {
		try {
			int _type = EDGE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:205:5: ( E D G E )
			// Dot.g:205:7: E D G E
			{
			mE(); 

			mD(); 

			mG(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "EDGE"

	// $ANTLR start "SUBGRAPH"
	public final void mSUBGRAPH() throws RecognitionException {
		try {
			int _type = SUBGRAPH;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:206:9: ( S U B G R A P H )
			// Dot.g:206:11: S U B G R A P H
			{
			mS(); 

			mU(); 

			mB(); 

			mG(); 

			mR(); 

			mA(); 

			mP(); 

			mH(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "SUBGRAPH"

	// $ANTLR start "EDGEOP"
	public final void mEDGEOP() throws RecognitionException {
		try {
			int _type = EDGEOP;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:208:8: ( '->' | '--' )
			int alt1=2;
			int LA1_0 = input.LA(1);
			if ( (LA1_0=='-') ) {
				int LA1_1 = input.LA(2);
				if ( (LA1_1=='>') ) {
					alt1=1;
				}
				else if ( (LA1_1=='-') ) {
					alt1=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 1, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 1, 0, input);
				throw nvae;
			}

			switch (alt1) {
				case 1 :
					// Dot.g:208:10: '->'
					{
					match("->"); 

					}
					break;
				case 2 :
					// Dot.g:208:17: '--'
					{
					match("--"); 

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "EDGEOP"

	// $ANTLR start "ID"
	public final void mID() throws RecognitionException {
		try {
			int _type = ID;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:211:5: ( ( VALIDSTR | NUMBER | QUOTEDSTR | HTMLSTR ) )
			// Dot.g:211:8: ( VALIDSTR | NUMBER | QUOTEDSTR | HTMLSTR )
			{
			// Dot.g:211:8: ( VALIDSTR | NUMBER | QUOTEDSTR | HTMLSTR )
			int alt2=4;
			switch ( input.LA(1) ) {
			case 'A':
			case 'B':
			case 'C':
			case 'D':
			case 'E':
			case 'F':
			case 'G':
			case 'H':
			case 'I':
			case 'J':
			case 'K':
			case 'L':
			case 'M':
			case 'N':
			case 'O':
			case 'P':
			case 'Q':
			case 'R':
			case 'S':
			case 'T':
			case 'U':
			case 'V':
			case 'W':
			case 'X':
			case 'Y':
			case 'Z':
			case '_':
			case 'a':
			case 'b':
			case 'c':
			case 'd':
			case 'e':
			case 'f':
			case 'g':
			case 'h':
			case 'i':
			case 'j':
			case 'k':
			case 'l':
			case 'm':
			case 'n':
			case 'o':
			case 'p':
			case 'q':
			case 'r':
			case 's':
			case 't':
			case 'u':
			case 'v':
			case 'w':
			case 'x':
			case 'y':
			case 'z':
				{
				alt2=1;
				}
				break;
			case '-':
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				{
				alt2=2;
				}
				break;
			case '\"':
				{
				alt2=3;
				}
				break;
			case '<':
				{
				alt2=4;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 2, 0, input);
				throw nvae;
			}
			switch (alt2) {
				case 1 :
					// Dot.g:211:11: VALIDSTR
					{
					mVALIDSTR(); 

					}
					break;
				case 2 :
					// Dot.g:212:11: NUMBER
					{
					mNUMBER(); 

					}
					break;
				case 3 :
					// Dot.g:213:11: QUOTEDSTR
					{
					mQUOTEDSTR(); 

					}
					break;
				case 4 :
					// Dot.g:214:11: HTMLSTR
					{
					mHTMLSTR(); 

					}
					break;

			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "ID"

	// $ANTLR start "ALPHACHAR"
	public final void mALPHACHAR() throws RecognitionException {
		try {
			// Dot.g:220:2: ( ( 'a' .. 'z' | 'A' .. 'Z' | '_' ) )
			// Dot.g:
			{
			if ( (input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "ALPHACHAR"

	// $ANTLR start "VALIDSTR"
	public final void mVALIDSTR() throws RecognitionException {
		try {
			// Dot.g:227:5: ( ALPHACHAR ( ALPHACHAR | '0' .. '9' )* )
			// Dot.g:227:8: ALPHACHAR ( ALPHACHAR | '0' .. '9' )*
			{
			mALPHACHAR(); 

			// Dot.g:228:9: ( ALPHACHAR | '0' .. '9' )*
			loop3:
			while (true) {
				int alt3=2;
				int LA3_0 = input.LA(1);
				if ( ((LA3_0 >= '0' && LA3_0 <= '9')||(LA3_0 >= 'A' && LA3_0 <= 'Z')||LA3_0=='_'||(LA3_0 >= 'a' && LA3_0 <= 'z')) ) {
					alt3=1;
				}

				switch (alt3) {
				case 1 :
					// Dot.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					break loop3;
				}
			}

			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "VALIDSTR"

	// $ANTLR start "NUMBER"
	public final void mNUMBER() throws RecognitionException {
		try {
			// Dot.g:234:5: ( ( '-' )? ( '0' .. '9' )+ ( '.' ( '0' .. '9' )+ )? )
			// Dot.g:234:8: ( '-' )? ( '0' .. '9' )+ ( '.' ( '0' .. '9' )+ )?
			{
			// Dot.g:234:8: ( '-' )?
			int alt4=2;
			int LA4_0 = input.LA(1);
			if ( (LA4_0=='-') ) {
				alt4=1;
			}
			switch (alt4) {
				case 1 :
					// Dot.g:234:9: '-'
					{
					match('-'); 
					}
					break;

			}

			// Dot.g:234:15: ( '0' .. '9' )+
			int cnt5=0;
			loop5:
			while (true) {
				int alt5=2;
				int LA5_0 = input.LA(1);
				if ( ((LA5_0 >= '0' && LA5_0 <= '9')) ) {
					alt5=1;
				}

				switch (alt5) {
				case 1 :
					// Dot.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt5 >= 1 ) break loop5;
					EarlyExitException eee = new EarlyExitException(5, input);
					throw eee;
				}
				cnt5++;
			}

			// Dot.g:234:27: ( '.' ( '0' .. '9' )+ )?
			int alt7=2;
			int LA7_0 = input.LA(1);
			if ( (LA7_0=='.') ) {
				alt7=1;
			}
			switch (alt7) {
				case 1 :
					// Dot.g:234:28: '.' ( '0' .. '9' )+
					{
					match('.'); 
					// Dot.g:234:32: ( '0' .. '9' )+
					int cnt6=0;
					loop6:
					while (true) {
						int alt6=2;
						int LA6_0 = input.LA(1);
						if ( ((LA6_0 >= '0' && LA6_0 <= '9')) ) {
							alt6=1;
						}

						switch (alt6) {
						case 1 :
							// Dot.g:
							{
							if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							if ( cnt6 >= 1 ) break loop6;
							EarlyExitException eee = new EarlyExitException(6, input);
							throw eee;
						}
						cnt6++;
					}

					}
					break;

			}

			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "NUMBER"

	// $ANTLR start "QUOTEDSTR"
	public final void mQUOTEDSTR() throws RecognitionException {
		try {
			// Dot.g:238:5: ( '\"' STR '\"' )
			// Dot.g:238:8: '\"' STR '\"'
			{
			match('\"'); 
			mSTR(); 

			match('\"'); 
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "QUOTEDSTR"

	// $ANTLR start "STR"
	public final void mSTR() throws RecognitionException {
		try {
			// Dot.g:244:5: ( ( ESCAPE_SEQUENCE |~ ( '\\\\' | '\"' ) )* )
			// Dot.g:245:6: ( ESCAPE_SEQUENCE |~ ( '\\\\' | '\"' ) )*
			{
			// Dot.g:245:6: ( ESCAPE_SEQUENCE |~ ( '\\\\' | '\"' ) )*
			loop8:
			while (true) {
				int alt8=3;
				int LA8_0 = input.LA(1);
				if ( (LA8_0=='\\') ) {
					alt8=1;
				}
				else if ( ((LA8_0 >= '\u0000' && LA8_0 <= '!')||(LA8_0 >= '#' && LA8_0 <= '[')||(LA8_0 >= ']' && LA8_0 <= '\uFFFF')) ) {
					alt8=2;
				}

				switch (alt8) {
				case 1 :
					// Dot.g:245:7: ESCAPE_SEQUENCE
					{
					mESCAPE_SEQUENCE(); 

					}
					break;
				case 2 :
					// Dot.g:245:25: ~ ( '\\\\' | '\"' )
					{
					if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '!')||(input.LA(1) >= '#' && input.LA(1) <= '[')||(input.LA(1) >= ']' && input.LA(1) <= '\uFFFF') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					break loop8;
				}
			}

			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "STR"

	// $ANTLR start "ESCAPE_SEQUENCE"
	public final void mESCAPE_SEQUENCE() throws RecognitionException {
		try {
			// Dot.g:249:5: ( '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' ) )
			// Dot.g:249:9: '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' )
			{
			match('\\'); 
			if ( input.LA(1)=='\"'||input.LA(1)=='\''||input.LA(1)=='\\'||input.LA(1)=='b'||input.LA(1)=='f'||input.LA(1)=='n'||input.LA(1)=='r'||input.LA(1)=='t' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "ESCAPE_SEQUENCE"

	// $ANTLR start "HTMLSTR"
	public final void mHTMLSTR() throws RecognitionException {
		try {
			// Dot.g:253:5: ( '<' (~ '>' )* '>' )
			// Dot.g:253:8: '<' (~ '>' )* '>'
			{
			match('<'); 
			// Dot.g:253:12: (~ '>' )*
			loop9:
			while (true) {
				int alt9=2;
				int LA9_0 = input.LA(1);
				if ( ((LA9_0 >= '\u0000' && LA9_0 <= '=')||(LA9_0 >= '?' && LA9_0 <= '\uFFFF')) ) {
					alt9=1;
				}

				switch (alt9) {
				case 1 :
					// Dot.g:
					{
					if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '=')||(input.LA(1) >= '?' && input.LA(1) <= '\uFFFF') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					break loop9;
				}
			}

			match('>'); 
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "HTMLSTR"

	// $ANTLR start "WS"
	public final void mWS() throws RecognitionException {
		try {
			int _type = WS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:256:5: ( ( ' ' | '\\t' )+ )
			// Dot.g:256:8: ( ' ' | '\\t' )+
			{
			// Dot.g:256:8: ( ' ' | '\\t' )+
			int cnt10=0;
			loop10:
			while (true) {
				int alt10=2;
				int LA10_0 = input.LA(1);
				if ( (LA10_0=='\t'||LA10_0==' ') ) {
					alt10=1;
				}

				switch (alt10) {
				case 1 :
					// Dot.g:
					{
					if ( input.LA(1)=='\t'||input.LA(1)==' ' ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt10 >= 1 ) break loop10;
					EarlyExitException eee = new EarlyExitException(10, input);
					throw eee;
				}
				cnt10++;
			}


			        _channel = HIDDEN;
			    
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "WS"

	// $ANTLR start "NEWLINE"
	public final void mNEWLINE() throws RecognitionException {
		try {
			int _type = NEWLINE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:262:10: ( ( '\\r' '\\n' | '\\r' | '\\n' | '\\u000C' ) )
			// Dot.g:262:13: ( '\\r' '\\n' | '\\r' | '\\n' | '\\u000C' )
			{
			// Dot.g:262:13: ( '\\r' '\\n' | '\\r' | '\\n' | '\\u000C' )
			int alt11=4;
			switch ( input.LA(1) ) {
			case '\r':
				{
				int LA11_1 = input.LA(2);
				if ( (LA11_1=='\n') ) {
					alt11=1;
				}

				else {
					alt11=2;
				}

				}
				break;
			case '\n':
				{
				alt11=3;
				}
				break;
			case '\f':
				{
				alt11=4;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 11, 0, input);
				throw nvae;
			}
			switch (alt11) {
				case 1 :
					// Dot.g:262:14: '\\r' '\\n'
					{
					match('\r'); 
					match('\n'); 
					}
					break;
				case 2 :
					// Dot.g:262:24: '\\r'
					{
					match('\r'); 
					}
					break;
				case 3 :
					// Dot.g:262:29: '\\n'
					{
					match('\n'); 
					}
					break;
				case 4 :
					// Dot.g:262:34: '\\u000C'
					{
					match('\f'); 
					}
					break;

			}


			        _channel = HIDDEN;
			    
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "NEWLINE"

	// $ANTLR start "COMMENT"
	public final void mCOMMENT() throws RecognitionException {
		try {
			int _type = COMMENT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:269:5: ( '/*' ( options {greedy=false; } : . )* '*/' )
			// Dot.g:269:9: '/*' ( options {greedy=false; } : . )* '*/'
			{
			match("/*"); 

			// Dot.g:269:14: ( options {greedy=false; } : . )*
			loop12:
			while (true) {
				int alt12=2;
				int LA12_0 = input.LA(1);
				if ( (LA12_0=='*') ) {
					int LA12_1 = input.LA(2);
					if ( (LA12_1=='/') ) {
						alt12=2;
					}
					else if ( ((LA12_1 >= '\u0000' && LA12_1 <= '.')||(LA12_1 >= '0' && LA12_1 <= '\uFFFF')) ) {
						alt12=1;
					}

				}
				else if ( ((LA12_0 >= '\u0000' && LA12_0 <= ')')||(LA12_0 >= '+' && LA12_0 <= '\uFFFF')) ) {
					alt12=1;
				}

				switch (alt12) {
				case 1 :
					// Dot.g:269:42: .
					{
					matchAny(); 
					}
					break;

				default :
					break loop12;
				}
			}

			match("*/"); 


			        _channel = HIDDEN;
			    
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "COMMENT"

	// $ANTLR start "LINE_COMMENT"
	public final void mLINE_COMMENT() throws RecognitionException {
		try {
			int _type = LINE_COMMENT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// Dot.g:276:5: ( '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n' )
			// Dot.g:276:7: '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n'
			{
			match("//"); 

			// Dot.g:276:12: (~ ( '\\n' | '\\r' ) )*
			loop13:
			while (true) {
				int alt13=2;
				int LA13_0 = input.LA(1);
				if ( ((LA13_0 >= '\u0000' && LA13_0 <= '\t')||(LA13_0 >= '\u000B' && LA13_0 <= '\f')||(LA13_0 >= '\u000E' && LA13_0 <= '\uFFFF')) ) {
					alt13=1;
				}

				switch (alt13) {
				case 1 :
					// Dot.g:
					{
					if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '\t')||(input.LA(1) >= '\u000B' && input.LA(1) <= '\f')||(input.LA(1) >= '\u000E' && input.LA(1) <= '\uFFFF') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					break loop13;
				}
			}

			// Dot.g:276:26: ( '\\r' )?
			int alt14=2;
			int LA14_0 = input.LA(1);
			if ( (LA14_0=='\r') ) {
				alt14=1;
			}
			switch (alt14) {
				case 1 :
					// Dot.g:276:26: '\\r'
					{
					match('\r'); 
					}
					break;

			}

			match('\n'); 

			        _channel = HIDDEN;
			    
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "LINE_COMMENT"

	// $ANTLR start "A"
	public final void mA() throws RecognitionException {
		try {
			// Dot.g:282:11: ( ( 'a' | 'A' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='A'||input.LA(1)=='a' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "A"

	// $ANTLR start "B"
	public final void mB() throws RecognitionException {
		try {
			// Dot.g:283:11: ( ( 'b' | 'B' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='B'||input.LA(1)=='b' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "B"

	// $ANTLR start "C"
	public final void mC() throws RecognitionException {
		try {
			// Dot.g:284:11: ( ( 'c' | 'C' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='C'||input.LA(1)=='c' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "C"

	// $ANTLR start "D"
	public final void mD() throws RecognitionException {
		try {
			// Dot.g:285:11: ( ( 'd' | 'D' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='D'||input.LA(1)=='d' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "D"

	// $ANTLR start "E"
	public final void mE() throws RecognitionException {
		try {
			// Dot.g:286:11: ( ( 'e' | 'E' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "E"

	// $ANTLR start "F"
	public final void mF() throws RecognitionException {
		try {
			// Dot.g:287:11: ( ( 'f' | 'F' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='F'||input.LA(1)=='f' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "F"

	// $ANTLR start "G"
	public final void mG() throws RecognitionException {
		try {
			// Dot.g:288:11: ( ( 'g' | 'G' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='G'||input.LA(1)=='g' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "G"

	// $ANTLR start "H"
	public final void mH() throws RecognitionException {
		try {
			// Dot.g:289:11: ( ( 'h' | 'H' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='H'||input.LA(1)=='h' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "H"

	// $ANTLR start "I"
	public final void mI() throws RecognitionException {
		try {
			// Dot.g:290:11: ( ( 'i' | 'I' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='I'||input.LA(1)=='i' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "I"

	// $ANTLR start "J"
	public final void mJ() throws RecognitionException {
		try {
			// Dot.g:291:11: ( ( 'j' | 'J' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='J'||input.LA(1)=='j' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "J"

	// $ANTLR start "K"
	public final void mK() throws RecognitionException {
		try {
			// Dot.g:292:11: ( ( 'k' | 'K' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='K'||input.LA(1)=='k' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K"

	// $ANTLR start "L"
	public final void mL() throws RecognitionException {
		try {
			// Dot.g:293:11: ( ( 'l' | 'L' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='L'||input.LA(1)=='l' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "L"

	// $ANTLR start "M"
	public final void mM() throws RecognitionException {
		try {
			// Dot.g:294:11: ( ( 'm' | 'M' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='M'||input.LA(1)=='m' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "M"

	// $ANTLR start "N"
	public final void mN() throws RecognitionException {
		try {
			// Dot.g:295:11: ( ( 'n' | 'N' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='N'||input.LA(1)=='n' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "N"

	// $ANTLR start "O"
	public final void mO() throws RecognitionException {
		try {
			// Dot.g:296:11: ( ( 'o' | 'O' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='O'||input.LA(1)=='o' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "O"

	// $ANTLR start "P"
	public final void mP() throws RecognitionException {
		try {
			// Dot.g:297:11: ( ( 'p' | 'P' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='P'||input.LA(1)=='p' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "P"

	// $ANTLR start "Q"
	public final void mQ() throws RecognitionException {
		try {
			// Dot.g:298:11: ( ( 'q' | 'Q' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='Q'||input.LA(1)=='q' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Q"

	// $ANTLR start "R"
	public final void mR() throws RecognitionException {
		try {
			// Dot.g:299:11: ( ( 'r' | 'R' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='R'||input.LA(1)=='r' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "R"

	// $ANTLR start "S"
	public final void mS() throws RecognitionException {
		try {
			// Dot.g:300:11: ( ( 's' | 'S' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='S'||input.LA(1)=='s' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "S"

	// $ANTLR start "T"
	public final void mT() throws RecognitionException {
		try {
			// Dot.g:301:11: ( ( 't' | 'T' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='T'||input.LA(1)=='t' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T"

	// $ANTLR start "U"
	public final void mU() throws RecognitionException {
		try {
			// Dot.g:302:11: ( ( 'u' | 'U' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='U'||input.LA(1)=='u' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "U"

	// $ANTLR start "V"
	public final void mV() throws RecognitionException {
		try {
			// Dot.g:303:11: ( ( 'v' | 'V' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='V'||input.LA(1)=='v' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "V"

	// $ANTLR start "W"
	public final void mW() throws RecognitionException {
		try {
			// Dot.g:304:11: ( ( 'w' | 'W' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='W'||input.LA(1)=='w' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "W"

	// $ANTLR start "X"
	public final void mX() throws RecognitionException {
		try {
			// Dot.g:305:11: ( ( 'x' | 'X' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='X'||input.LA(1)=='x' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "X"

	// $ANTLR start "Y"
	public final void mY() throws RecognitionException {
		try {
			// Dot.g:306:11: ( ( 'y' | 'Y' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='Y'||input.LA(1)=='y' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Y"

	// $ANTLR start "Z"
	public final void mZ() throws RecognitionException {
		try {
			// Dot.g:307:11: ( ( 'z' | 'Z' ) )
			// Dot.g:
			{
			if ( input.LA(1)=='Z'||input.LA(1)=='z' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Z"

	@Override
	public void mTokens() throws RecognitionException {
		// Dot.g:1:8: ( COLON | COMMA | C_BRACKET | C_SQR_BRACKET | EQUAL | LPAREN | O_BRACKET | O_SQR_BRACKET | RPAREN | SEMI_COLON | GRAPH | DIGRAPH | STRICT | NODE | EDGE | SUBGRAPH | EDGEOP | ID | WS | NEWLINE | COMMENT | LINE_COMMENT )
		int alt15=22;
		switch ( input.LA(1) ) {
		case ':':
			{
			alt15=1;
			}
			break;
		case ',':
			{
			alt15=2;
			}
			break;
		case '}':
			{
			alt15=3;
			}
			break;
		case ']':
			{
			alt15=4;
			}
			break;
		case '=':
			{
			alt15=5;
			}
			break;
		case '(':
			{
			alt15=6;
			}
			break;
		case '{':
			{
			alt15=7;
			}
			break;
		case '[':
			{
			alt15=8;
			}
			break;
		case ')':
			{
			alt15=9;
			}
			break;
		case ';':
			{
			alt15=10;
			}
			break;
		case 'G':
		case 'g':
			{
			int LA15_11 = input.LA(2);
			if ( (LA15_11=='R'||LA15_11=='r') ) {
				int LA15_21 = input.LA(3);
				if ( (LA15_21=='A'||LA15_21=='a') ) {
					int LA15_30 = input.LA(4);
					if ( (LA15_30=='P'||LA15_30=='p') ) {
						int LA15_36 = input.LA(5);
						if ( (LA15_36=='H'||LA15_36=='h') ) {
							int LA15_42 = input.LA(6);
							if ( ((LA15_42 >= '0' && LA15_42 <= '9')||(LA15_42 >= 'A' && LA15_42 <= 'Z')||LA15_42=='_'||(LA15_42 >= 'a' && LA15_42 <= 'z')) ) {
								alt15=18;
							}

							else {
								alt15=11;
							}

						}

						else {
							alt15=18;
						}

					}

					else {
						alt15=18;
					}

				}

				else {
					alt15=18;
				}

			}

			else {
				alt15=18;
			}

			}
			break;
		case 'D':
		case 'd':
			{
			int LA15_12 = input.LA(2);
			if ( (LA15_12=='I'||LA15_12=='i') ) {
				int LA15_22 = input.LA(3);
				if ( (LA15_22=='G'||LA15_22=='g') ) {
					int LA15_31 = input.LA(4);
					if ( (LA15_31=='R'||LA15_31=='r') ) {
						int LA15_37 = input.LA(5);
						if ( (LA15_37=='A'||LA15_37=='a') ) {
							int LA15_43 = input.LA(6);
							if ( (LA15_43=='P'||LA15_43=='p') ) {
								int LA15_49 = input.LA(7);
								if ( (LA15_49=='H'||LA15_49=='h') ) {
									int LA15_52 = input.LA(8);
									if ( ((LA15_52 >= '0' && LA15_52 <= '9')||(LA15_52 >= 'A' && LA15_52 <= 'Z')||LA15_52=='_'||(LA15_52 >= 'a' && LA15_52 <= 'z')) ) {
										alt15=18;
									}

									else {
										alt15=12;
									}

								}

								else {
									alt15=18;
								}

							}

							else {
								alt15=18;
							}

						}

						else {
							alt15=18;
						}

					}

					else {
						alt15=18;
					}

				}

				else {
					alt15=18;
				}

			}

			else {
				alt15=18;
			}

			}
			break;
		case 'S':
		case 's':
			{
			switch ( input.LA(2) ) {
			case 'T':
			case 't':
				{
				int LA15_23 = input.LA(3);
				if ( (LA15_23=='R'||LA15_23=='r') ) {
					int LA15_32 = input.LA(4);
					if ( (LA15_32=='I'||LA15_32=='i') ) {
						int LA15_38 = input.LA(5);
						if ( (LA15_38=='C'||LA15_38=='c') ) {
							int LA15_44 = input.LA(6);
							if ( (LA15_44=='T'||LA15_44=='t') ) {
								int LA15_50 = input.LA(7);
								if ( ((LA15_50 >= '0' && LA15_50 <= '9')||(LA15_50 >= 'A' && LA15_50 <= 'Z')||LA15_50=='_'||(LA15_50 >= 'a' && LA15_50 <= 'z')) ) {
									alt15=18;
								}

								else {
									alt15=13;
								}

							}

							else {
								alt15=18;
							}

						}

						else {
							alt15=18;
						}

					}

					else {
						alt15=18;
					}

				}

				else {
					alt15=18;
				}

				}
				break;
			case 'U':
			case 'u':
				{
				int LA15_24 = input.LA(3);
				if ( (LA15_24=='B'||LA15_24=='b') ) {
					int LA15_33 = input.LA(4);
					if ( (LA15_33=='G'||LA15_33=='g') ) {
						int LA15_39 = input.LA(5);
						if ( (LA15_39=='R'||LA15_39=='r') ) {
							int LA15_45 = input.LA(6);
							if ( (LA15_45=='A'||LA15_45=='a') ) {
								int LA15_51 = input.LA(7);
								if ( (LA15_51=='P'||LA15_51=='p') ) {
									int LA15_54 = input.LA(8);
									if ( (LA15_54=='H'||LA15_54=='h') ) {
										int LA15_56 = input.LA(9);
										if ( ((LA15_56 >= '0' && LA15_56 <= '9')||(LA15_56 >= 'A' && LA15_56 <= 'Z')||LA15_56=='_'||(LA15_56 >= 'a' && LA15_56 <= 'z')) ) {
											alt15=18;
										}

										else {
											alt15=16;
										}

									}

									else {
										alt15=18;
									}

								}

								else {
									alt15=18;
								}

							}

							else {
								alt15=18;
							}

						}

						else {
							alt15=18;
						}

					}

					else {
						alt15=18;
					}

				}

				else {
					alt15=18;
				}

				}
				break;
			default:
				alt15=18;
			}
			}
			break;
		case 'N':
		case 'n':
			{
			int LA15_14 = input.LA(2);
			if ( (LA15_14=='O'||LA15_14=='o') ) {
				int LA15_25 = input.LA(3);
				if ( (LA15_25=='D'||LA15_25=='d') ) {
					int LA15_34 = input.LA(4);
					if ( (LA15_34=='E'||LA15_34=='e') ) {
						int LA15_40 = input.LA(5);
						if ( ((LA15_40 >= '0' && LA15_40 <= '9')||(LA15_40 >= 'A' && LA15_40 <= 'Z')||LA15_40=='_'||(LA15_40 >= 'a' && LA15_40 <= 'z')) ) {
							alt15=18;
						}

						else {
							alt15=14;
						}

					}

					else {
						alt15=18;
					}

				}

				else {
					alt15=18;
				}

			}

			else {
				alt15=18;
			}

			}
			break;
		case 'E':
		case 'e':
			{
			int LA15_15 = input.LA(2);
			if ( (LA15_15=='D'||LA15_15=='d') ) {
				int LA15_26 = input.LA(3);
				if ( (LA15_26=='G'||LA15_26=='g') ) {
					int LA15_35 = input.LA(4);
					if ( (LA15_35=='E'||LA15_35=='e') ) {
						int LA15_41 = input.LA(5);
						if ( ((LA15_41 >= '0' && LA15_41 <= '9')||(LA15_41 >= 'A' && LA15_41 <= 'Z')||LA15_41=='_'||(LA15_41 >= 'a' && LA15_41 <= 'z')) ) {
							alt15=18;
						}

						else {
							alt15=15;
						}

					}

					else {
						alt15=18;
					}

				}

				else {
					alt15=18;
				}

			}

			else {
				alt15=18;
			}

			}
			break;
		case '-':
			{
			int LA15_16 = input.LA(2);
			if ( (LA15_16=='-'||LA15_16=='>') ) {
				alt15=17;
			}
			else if ( ((LA15_16 >= '0' && LA15_16 <= '9')) ) {
				alt15=18;
			}

			else {
				int nvaeMark = input.mark();
				try {
					input.consume();
					NoViableAltException nvae =
						new NoViableAltException("", 15, 16, input);
					throw nvae;
				} finally {
					input.rewind(nvaeMark);
				}
			}

			}
			break;
		case '\"':
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
		case '<':
		case 'A':
		case 'B':
		case 'C':
		case 'F':
		case 'H':
		case 'I':
		case 'J':
		case 'K':
		case 'L':
		case 'M':
		case 'O':
		case 'P':
		case 'Q':
		case 'R':
		case 'T':
		case 'U':
		case 'V':
		case 'W':
		case 'X':
		case 'Y':
		case 'Z':
		case '_':
		case 'a':
		case 'b':
		case 'c':
		case 'f':
		case 'h':
		case 'i':
		case 'j':
		case 'k':
		case 'l':
		case 'm':
		case 'o':
		case 'p':
		case 'q':
		case 'r':
		case 't':
		case 'u':
		case 'v':
		case 'w':
		case 'x':
		case 'y':
		case 'z':
			{
			alt15=18;
			}
			break;
		case '\t':
		case ' ':
			{
			alt15=19;
			}
			break;
		case '\n':
		case '\f':
		case '\r':
			{
			alt15=20;
			}
			break;
		case '/':
			{
			int LA15_20 = input.LA(2);
			if ( (LA15_20=='*') ) {
				alt15=21;
			}
			else if ( (LA15_20=='/') ) {
				alt15=22;
			}

			else {
				int nvaeMark = input.mark();
				try {
					input.consume();
					NoViableAltException nvae =
						new NoViableAltException("", 15, 20, input);
					throw nvae;
				} finally {
					input.rewind(nvaeMark);
				}
			}

			}
			break;
		default:
			NoViableAltException nvae =
				new NoViableAltException("", 15, 0, input);
			throw nvae;
		}
		switch (alt15) {
			case 1 :
				// Dot.g:1:10: COLON
				{
				mCOLON(); 

				}
				break;
			case 2 :
				// Dot.g:1:16: COMMA
				{
				mCOMMA(); 

				}
				break;
			case 3 :
				// Dot.g:1:22: C_BRACKET
				{
				mC_BRACKET(); 

				}
				break;
			case 4 :
				// Dot.g:1:32: C_SQR_BRACKET
				{
				mC_SQR_BRACKET(); 

				}
				break;
			case 5 :
				// Dot.g:1:46: EQUAL
				{
				mEQUAL(); 

				}
				break;
			case 6 :
				// Dot.g:1:52: LPAREN
				{
				mLPAREN(); 

				}
				break;
			case 7 :
				// Dot.g:1:59: O_BRACKET
				{
				mO_BRACKET(); 

				}
				break;
			case 8 :
				// Dot.g:1:69: O_SQR_BRACKET
				{
				mO_SQR_BRACKET(); 

				}
				break;
			case 9 :
				// Dot.g:1:83: RPAREN
				{
				mRPAREN(); 

				}
				break;
			case 10 :
				// Dot.g:1:90: SEMI_COLON
				{
				mSEMI_COLON(); 

				}
				break;
			case 11 :
				// Dot.g:1:101: GRAPH
				{
				mGRAPH(); 

				}
				break;
			case 12 :
				// Dot.g:1:107: DIGRAPH
				{
				mDIGRAPH(); 

				}
				break;
			case 13 :
				// Dot.g:1:115: STRICT
				{
				mSTRICT(); 

				}
				break;
			case 14 :
				// Dot.g:1:122: NODE
				{
				mNODE(); 

				}
				break;
			case 15 :
				// Dot.g:1:127: EDGE
				{
				mEDGE(); 

				}
				break;
			case 16 :
				// Dot.g:1:132: SUBGRAPH
				{
				mSUBGRAPH(); 

				}
				break;
			case 17 :
				// Dot.g:1:141: EDGEOP
				{
				mEDGEOP(); 

				}
				break;
			case 18 :
				// Dot.g:1:148: ID
				{
				mID(); 

				}
				break;
			case 19 :
				// Dot.g:1:151: WS
				{
				mWS(); 

				}
				break;
			case 20 :
				// Dot.g:1:154: NEWLINE
				{
				mNEWLINE(); 

				}
				break;
			case 21 :
				// Dot.g:1:162: COMMENT
				{
				mCOMMENT(); 

				}
				break;
			case 22 :
				// Dot.g:1:170: LINE_COMMENT
				{
				mLINE_COMMENT(); 

				}
				break;

		}
	}



}
