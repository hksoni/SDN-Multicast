// $ANTLR 3.5.2 Dot.g 2015-04-28 15:52:36

package org.cesta.parsers.dot;

import java.util.logging.Logger;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;


/**
 *  An ANTLRv3 capable DOT grammar. 
 *
 *  Developed from specification on http://www.graphviz.org/doc/info/lang.html
 *	This grammar is part of CesTa project, http://cesta.sourceforge.net/
 *
 *	BSD licence
 *  Copyright (c) 2010 Tobias Smolka, BUSLAB FI MUNI 
 *
 *	All rights reserved.
 *
 *	http://buslab.org
 *
 *	Redistribution and use in source and binary forms, with or without
 *	modification, are permitted provided that the following conditions
 *	are met:
 *
 *	 1. Redistributions of source code must retain the above copyright
 *		notice, this list of conditions and the following disclaimer.
 *	 2. Redistributions in binary form must reproduce the above copyright
 *		notice, this list of conditions and the following disclaimer in the
 *		documentation and/or other materials provided with the distribution.
 *	 3. The name of the author may not be used to endorse or promote products
 *		derived from this software without specific prior written permission.
 *
 *	THIS SOFTWARE IS PROVIDED BY BUSLAB FI MUNI ('BUSLAB') ``AS IS'' 
 *	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 *	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 *	ARE DISCLAIMED. IN NO EVENT SHALL 'BUSLAB' BE LIABLE FOR ANY DIRECT, INDIRECT, 
 *	INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
 *	LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 *	OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
 *	LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 *	NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, 
 *	EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
@SuppressWarnings("all")
public class DotParser extends Parser {
	public static final String[] tokenNames = new String[] {
		"<invalid>", "<EOR>", "<DOWN>", "<UP>", "A", "ALPHACHAR", "ATTR", "ATTR_LIST", 
		"B", "C", "COLON", "COMMA", "COMMENT", "C_BRACKET", "C_SQR_BRACKET", "D", 
		"DIGRAPH", "E", "EDGE", "EDGEOP", "EDGE_STMT", "EQUAL", "ESCAPE_SEQUENCE", 
		"F", "G", "GRAPH", "GRAPH_ROOT", "H", "HTMLSTR", "I", "ID", "J", "K", 
		"L", "LINE_COMMENT", "LPAREN", "M", "N", "NEWLINE", "NODE", "NODE_STMT", 
		"NUMBER", "O", "O_BRACKET", "O_SQR_BRACKET", "P", "Q", "QUOTEDSTR", "R", 
		"RPAREN", "S", "SEMI_COLON", "STMT_LIST", "STR", "STRICT", "SUBGRAPH", 
		"SUBGRAPH_ROOT", "T", "U", "V", "VALIDSTR", "W", "WS", "X", "Y", "Z"
	};
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
	public Parser[] getDelegates() {
		return new Parser[] {};
	}

	// delegators


	public DotParser(TokenStream input) {
		this(input, new RecognizerSharedState());
	}
	public DotParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);
		this.state.ruleMemo = new HashMap[48+1];


	}

	protected TreeAdaptor adaptor = new CommonTreeAdaptor();

	public void setTreeAdaptor(TreeAdaptor adaptor) {
		this.adaptor = adaptor;
	}
	public TreeAdaptor getTreeAdaptor() {
		return adaptor;
	}
	@Override public String[] getTokenNames() { return DotParser.tokenNames; }
	@Override public String getGrammarFileName() { return "Dot.g"; }


	    private boolean hasErrors = false;
	    private Logger logger = Logger.getLogger(this.getClass().getName());

	    public void setLogger(Logger newLogger){
	        logger = newLogger;
	    }

	    @Override
	    public void emitErrorMessage(String message) {
	        hasErrors = true;
		if (logger!=null) logger.warning(message);
	        super.emitErrorMessage(message);
	    }

	    public boolean hasErrors(){
	        return hasErrors;
	    }


	public static class graph_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "graph"
	// Dot.g:110:1: graph : graphModifier ( ID )? O_BRACKET stmt_list C_BRACKET -> ^( GRAPH_ROOT graphModifier ( ID )? stmt_list ) ;
	public final DotParser.graph_return graph() throws RecognitionException {
		DotParser.graph_return retval = new DotParser.graph_return();
		retval.start = input.LT(1);
		int graph_StartIndex = input.index();

		CommonTree root_0 = null;

		Token ID2=null;
		Token O_BRACKET3=null;
		Token C_BRACKET5=null;
		ParserRuleReturnScope graphModifier1 =null;
		ParserRuleReturnScope stmt_list4 =null;

		CommonTree ID2_tree=null;
		CommonTree O_BRACKET3_tree=null;
		CommonTree C_BRACKET5_tree=null;
		RewriteRuleTokenStream stream_C_BRACKET=new RewriteRuleTokenStream(adaptor,"token C_BRACKET");
		RewriteRuleTokenStream stream_O_BRACKET=new RewriteRuleTokenStream(adaptor,"token O_BRACKET");
		RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
		RewriteRuleSubtreeStream stream_graphModifier=new RewriteRuleSubtreeStream(adaptor,"rule graphModifier");
		RewriteRuleSubtreeStream stream_stmt_list=new RewriteRuleSubtreeStream(adaptor,"rule stmt_list");

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 1) ) { return retval; }

			// Dot.g:111:5: ( graphModifier ( ID )? O_BRACKET stmt_list C_BRACKET -> ^( GRAPH_ROOT graphModifier ( ID )? stmt_list ) )
			// Dot.g:112:6: graphModifier ( ID )? O_BRACKET stmt_list C_BRACKET
			{
			pushFollow(FOLLOW_graphModifier_in_graph327);
			graphModifier1=graphModifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_graphModifier.add(graphModifier1.getTree());
			// Dot.g:112:20: ( ID )?
			int alt1=2;
			int LA1_0 = input.LA(1);
			if ( (LA1_0==ID) ) {
				alt1=1;
			}
			switch (alt1) {
				case 1 :
					// Dot.g:112:20: ID
					{
					ID2=(Token)match(input,ID,FOLLOW_ID_in_graph329); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_ID.add(ID2);

					}
					break;

			}

			O_BRACKET3=(Token)match(input,O_BRACKET,FOLLOW_O_BRACKET_in_graph332); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_O_BRACKET.add(O_BRACKET3);

			pushFollow(FOLLOW_stmt_list_in_graph334);
			stmt_list4=stmt_list();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_stmt_list.add(stmt_list4.getTree());
			C_BRACKET5=(Token)match(input,C_BRACKET,FOLLOW_C_BRACKET_in_graph336); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_C_BRACKET.add(C_BRACKET5);

			// AST REWRITE
			// elements: ID, graphModifier, stmt_list
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 113:6: -> ^( GRAPH_ROOT graphModifier ( ID )? stmt_list )
			{
				// Dot.g:113:9: ^( GRAPH_ROOT graphModifier ( ID )? stmt_list )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(GRAPH_ROOT, "GRAPH_ROOT"), root_1);
				adaptor.addChild(root_1, stream_graphModifier.nextTree());
				// Dot.g:113:36: ( ID )?
				if ( stream_ID.hasNext() ) {
					adaptor.addChild(root_1, stream_ID.nextNode());
				}
				stream_ID.reset();

				adaptor.addChild(root_1, stream_stmt_list.nextTree());
				adaptor.addChild(root_0, root_1);
				}

			}


			retval.tree = root_0;
			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 1, graph_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "graph"


	public static class graphModifier_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "graphModifier"
	// Dot.g:116:1: graphModifier : ( STRICT )? ( GRAPH | DIGRAPH ) ;
	public final DotParser.graphModifier_return graphModifier() throws RecognitionException {
		DotParser.graphModifier_return retval = new DotParser.graphModifier_return();
		retval.start = input.LT(1);
		int graphModifier_StartIndex = input.index();

		CommonTree root_0 = null;

		Token STRICT6=null;
		Token set7=null;

		CommonTree STRICT6_tree=null;
		CommonTree set7_tree=null;

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 2) ) { return retval; }

			// Dot.g:117:6: ( ( STRICT )? ( GRAPH | DIGRAPH ) )
			// Dot.g:118:7: ( STRICT )? ( GRAPH | DIGRAPH )
			{
			root_0 = (CommonTree)adaptor.nil();


			// Dot.g:118:7: ( STRICT )?
			int alt2=2;
			int LA2_0 = input.LA(1);
			if ( (LA2_0==STRICT) ) {
				alt2=1;
			}
			switch (alt2) {
				case 1 :
					// Dot.g:118:8: STRICT
					{
					STRICT6=(Token)match(input,STRICT,FOLLOW_STRICT_in_graphModifier385); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					STRICT6_tree = (CommonTree)adaptor.create(STRICT6);
					adaptor.addChild(root_0, STRICT6_tree);
					}

					}
					break;

			}

			set7=input.LT(1);
			if ( input.LA(1)==DIGRAPH||input.LA(1)==GRAPH ) {
				input.consume();
				if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set7));
				state.errorRecovery=false;
				state.failed=false;
			}
			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 2, graphModifier_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "graphModifier"


	public static class stmt_list_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "stmt_list"
	// Dot.g:121:1: stmt_list : stmt ( ( SEMI_COLON )* stmt )* ( SEMI_COLON )* -> ^( STMT_LIST ( stmt )+ ) ;
	public final DotParser.stmt_list_return stmt_list() throws RecognitionException {
		DotParser.stmt_list_return retval = new DotParser.stmt_list_return();
		retval.start = input.LT(1);
		int stmt_list_StartIndex = input.index();

		CommonTree root_0 = null;

		Token SEMI_COLON9=null;
		Token SEMI_COLON11=null;
		ParserRuleReturnScope stmt8 =null;
		ParserRuleReturnScope stmt10 =null;

		CommonTree SEMI_COLON9_tree=null;
		CommonTree SEMI_COLON11_tree=null;
		RewriteRuleTokenStream stream_SEMI_COLON=new RewriteRuleTokenStream(adaptor,"token SEMI_COLON");
		RewriteRuleSubtreeStream stream_stmt=new RewriteRuleSubtreeStream(adaptor,"rule stmt");

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 3) ) { return retval; }

			// Dot.g:122:5: ( stmt ( ( SEMI_COLON )* stmt )* ( SEMI_COLON )* -> ^( STMT_LIST ( stmt )+ ) )
			// Dot.g:122:8: stmt ( ( SEMI_COLON )* stmt )* ( SEMI_COLON )*
			{
			pushFollow(FOLLOW_stmt_in_stmt_list414);
			stmt8=stmt();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_stmt.add(stmt8.getTree());
			// Dot.g:122:13: ( ( SEMI_COLON )* stmt )*
			loop4:
			while (true) {
				int alt4=2;
				alt4 = dfa4.predict(input);
				switch (alt4) {
				case 1 :
					// Dot.g:122:14: ( SEMI_COLON )* stmt
					{
					// Dot.g:122:14: ( SEMI_COLON )*
					loop3:
					while (true) {
						int alt3=2;
						int LA3_0 = input.LA(1);
						if ( (LA3_0==SEMI_COLON) ) {
							alt3=1;
						}

						switch (alt3) {
						case 1 :
							// Dot.g:122:14: SEMI_COLON
							{
							SEMI_COLON9=(Token)match(input,SEMI_COLON,FOLLOW_SEMI_COLON_in_stmt_list417); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_SEMI_COLON.add(SEMI_COLON9);

							}
							break;

						default :
							break loop3;
						}
					}

					pushFollow(FOLLOW_stmt_in_stmt_list420);
					stmt10=stmt();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_stmt.add(stmt10.getTree());
					}
					break;

				default :
					break loop4;
				}
			}

			// Dot.g:122:33: ( SEMI_COLON )*
			loop5:
			while (true) {
				int alt5=2;
				int LA5_0 = input.LA(1);
				if ( (LA5_0==SEMI_COLON) ) {
					alt5=1;
				}

				switch (alt5) {
				case 1 :
					// Dot.g:122:33: SEMI_COLON
					{
					SEMI_COLON11=(Token)match(input,SEMI_COLON,FOLLOW_SEMI_COLON_in_stmt_list424); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_SEMI_COLON.add(SEMI_COLON11);

					}
					break;

				default :
					break loop5;
				}
			}

			// AST REWRITE
			// elements: stmt
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 122:45: -> ^( STMT_LIST ( stmt )+ )
			{
				// Dot.g:122:48: ^( STMT_LIST ( stmt )+ )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(STMT_LIST, "STMT_LIST"), root_1);
				if ( !(stream_stmt.hasNext()) ) {
					throw new RewriteEarlyExitException();
				}
				while ( stream_stmt.hasNext() ) {
					adaptor.addChild(root_1, stream_stmt.nextTree());
				}
				stream_stmt.reset();

				adaptor.addChild(root_0, root_1);
				}

			}


			retval.tree = root_0;
			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 3, stmt_list_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "stmt_list"


	public static class stmt_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "stmt"
	// Dot.g:125:1: stmt : ( attr_stmt | edge_stmt | subgraph | ID EQUAL ID -> ^( ATTR ID EQUAL ID ) | node_stmt );
	public final DotParser.stmt_return stmt() throws RecognitionException {
		DotParser.stmt_return retval = new DotParser.stmt_return();
		retval.start = input.LT(1);
		int stmt_StartIndex = input.index();

		CommonTree root_0 = null;

		Token ID15=null;
		Token EQUAL16=null;
		Token ID17=null;
		ParserRuleReturnScope attr_stmt12 =null;
		ParserRuleReturnScope edge_stmt13 =null;
		ParserRuleReturnScope subgraph14 =null;
		ParserRuleReturnScope node_stmt18 =null;

		CommonTree ID15_tree=null;
		CommonTree EQUAL16_tree=null;
		CommonTree ID17_tree=null;
		RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
		RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 4) ) { return retval; }

			// Dot.g:126:5: ( attr_stmt | edge_stmt | subgraph | ID EQUAL ID -> ^( ATTR ID EQUAL ID ) | node_stmt )
			int alt6=5;
			switch ( input.LA(1) ) {
			case EDGE:
			case GRAPH:
			case NODE:
				{
				alt6=1;
				}
				break;
			case ID:
				{
				int LA6_4 = input.LA(2);
				if ( (synpred8_Dot()) ) {
					alt6=2;
				}
				else if ( (synpred10_Dot()) ) {
					alt6=4;
				}
				else if ( (true) ) {
					alt6=5;
				}

				}
				break;
			case O_BRACKET:
				{
				int LA6_5 = input.LA(2);
				if ( (synpred8_Dot()) ) {
					alt6=2;
				}
				else if ( (synpred9_Dot()) ) {
					alt6=3;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 6, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case SUBGRAPH:
				{
				int LA6_6 = input.LA(2);
				if ( (synpred8_Dot()) ) {
					alt6=2;
				}
				else if ( (synpred9_Dot()) ) {
					alt6=3;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 6, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			default:
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 6, 0, input);
				throw nvae;
			}
			switch (alt6) {
				case 1 :
					// Dot.g:127:9: attr_stmt
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_attr_stmt_in_stmt461);
					attr_stmt12=attr_stmt();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, attr_stmt12.getTree());

					}
					break;
				case 2 :
					// Dot.g:128:9: edge_stmt
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_edge_stmt_in_stmt473);
					edge_stmt13=edge_stmt();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, edge_stmt13.getTree());

					}
					break;
				case 3 :
					// Dot.g:129:6: subgraph
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_subgraph_in_stmt482);
					subgraph14=subgraph();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, subgraph14.getTree());

					}
					break;
				case 4 :
					// Dot.g:130:6: ID EQUAL ID
					{
					ID15=(Token)match(input,ID,FOLLOW_ID_in_stmt491); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_ID.add(ID15);

					EQUAL16=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_stmt493); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL16);

					ID17=(Token)match(input,ID,FOLLOW_ID_in_stmt495); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_ID.add(ID17);

					// AST REWRITE
					// elements: EQUAL, ID, ID
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 130:19: -> ^( ATTR ID EQUAL ID )
					{
						// Dot.g:130:22: ^( ATTR ID EQUAL ID )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ATTR, "ATTR"), root_1);
						adaptor.addChild(root_1, stream_ID.nextNode());
						adaptor.addChild(root_1, stream_EQUAL.nextNode());
						adaptor.addChild(root_1, stream_ID.nextNode());
						adaptor.addChild(root_0, root_1);
						}

					}


					retval.tree = root_0;
					}

					}
					break;
				case 5 :
					// Dot.g:131:6: node_stmt
					{
					root_0 = (CommonTree)adaptor.nil();


					pushFollow(FOLLOW_node_stmt_in_stmt517);
					node_stmt18=node_stmt();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, node_stmt18.getTree());

					}
					break;

			}
			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 4, stmt_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "stmt"


	public static class attr_stmt_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "attr_stmt"
	// Dot.g:134:1: attr_stmt : ( GRAPH ^| NODE ^| EDGE ^) ( attr_list ) ;
	public final DotParser.attr_stmt_return attr_stmt() throws RecognitionException {
		DotParser.attr_stmt_return retval = new DotParser.attr_stmt_return();
		retval.start = input.LT(1);
		int attr_stmt_StartIndex = input.index();

		CommonTree root_0 = null;

		Token GRAPH19=null;
		Token NODE20=null;
		Token EDGE21=null;
		ParserRuleReturnScope attr_list22 =null;

		CommonTree GRAPH19_tree=null;
		CommonTree NODE20_tree=null;
		CommonTree EDGE21_tree=null;

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 5) ) { return retval; }

			// Dot.g:135:5: ( ( GRAPH ^| NODE ^| EDGE ^) ( attr_list ) )
			// Dot.g:135:8: ( GRAPH ^| NODE ^| EDGE ^) ( attr_list )
			{
			root_0 = (CommonTree)adaptor.nil();


			// Dot.g:135:8: ( GRAPH ^| NODE ^| EDGE ^)
			int alt7=3;
			switch ( input.LA(1) ) {
			case GRAPH:
				{
				alt7=1;
				}
				break;
			case NODE:
				{
				alt7=2;
				}
				break;
			case EDGE:
				{
				alt7=3;
				}
				break;
			default:
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 7, 0, input);
				throw nvae;
			}
			switch (alt7) {
				case 1 :
					// Dot.g:135:9: GRAPH ^
					{
					GRAPH19=(Token)match(input,GRAPH,FOLLOW_GRAPH_in_attr_stmt537); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					GRAPH19_tree = (CommonTree)adaptor.create(GRAPH19);
					root_0 = (CommonTree)adaptor.becomeRoot(GRAPH19_tree, root_0);
					}

					}
					break;
				case 2 :
					// Dot.g:135:18: NODE ^
					{
					NODE20=(Token)match(input,NODE,FOLLOW_NODE_in_attr_stmt542); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					NODE20_tree = (CommonTree)adaptor.create(NODE20);
					root_0 = (CommonTree)adaptor.becomeRoot(NODE20_tree, root_0);
					}

					}
					break;
				case 3 :
					// Dot.g:135:26: EDGE ^
					{
					EDGE21=(Token)match(input,EDGE,FOLLOW_EDGE_in_attr_stmt547); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					EDGE21_tree = (CommonTree)adaptor.create(EDGE21);
					root_0 = (CommonTree)adaptor.becomeRoot(EDGE21_tree, root_0);
					}

					}
					break;

			}

			// Dot.g:135:33: ( attr_list )
			// Dot.g:135:34: attr_list
			{
			pushFollow(FOLLOW_attr_list_in_attr_stmt552);
			attr_list22=attr_list();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) adaptor.addChild(root_0, attr_list22.getTree());

			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 5, attr_stmt_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "attr_stmt"


	public static class attr_list_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "attr_list"
	// Dot.g:138:1: attr_list : ( O_SQR_BRACKET ( a_list )? C_SQR_BRACKET )* -> ^( ATTR_LIST ( a_list )* ) ;
	public final DotParser.attr_list_return attr_list() throws RecognitionException {
		DotParser.attr_list_return retval = new DotParser.attr_list_return();
		retval.start = input.LT(1);
		int attr_list_StartIndex = input.index();

		CommonTree root_0 = null;

		Token O_SQR_BRACKET23=null;
		Token C_SQR_BRACKET25=null;
		ParserRuleReturnScope a_list24 =null;

		CommonTree O_SQR_BRACKET23_tree=null;
		CommonTree C_SQR_BRACKET25_tree=null;
		RewriteRuleTokenStream stream_O_SQR_BRACKET=new RewriteRuleTokenStream(adaptor,"token O_SQR_BRACKET");
		RewriteRuleTokenStream stream_C_SQR_BRACKET=new RewriteRuleTokenStream(adaptor,"token C_SQR_BRACKET");
		RewriteRuleSubtreeStream stream_a_list=new RewriteRuleSubtreeStream(adaptor,"rule a_list");

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 6) ) { return retval; }

			// Dot.g:139:5: ( ( O_SQR_BRACKET ( a_list )? C_SQR_BRACKET )* -> ^( ATTR_LIST ( a_list )* ) )
			// Dot.g:140:6: ( O_SQR_BRACKET ( a_list )? C_SQR_BRACKET )*
			{
			// Dot.g:140:6: ( O_SQR_BRACKET ( a_list )? C_SQR_BRACKET )*
			loop9:
			while (true) {
				int alt9=2;
				int LA9_0 = input.LA(1);
				if ( (LA9_0==O_SQR_BRACKET) ) {
					alt9=1;
				}

				switch (alt9) {
				case 1 :
					// Dot.g:140:7: O_SQR_BRACKET ( a_list )? C_SQR_BRACKET
					{
					O_SQR_BRACKET23=(Token)match(input,O_SQR_BRACKET,FOLLOW_O_SQR_BRACKET_in_attr_list578); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_O_SQR_BRACKET.add(O_SQR_BRACKET23);

					// Dot.g:140:21: ( a_list )?
					int alt8=2;
					int LA8_0 = input.LA(1);
					if ( (LA8_0==ID) ) {
						alt8=1;
					}
					switch (alt8) {
						case 1 :
							// Dot.g:140:21: a_list
							{
							pushFollow(FOLLOW_a_list_in_attr_list580);
							a_list24=a_list();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_a_list.add(a_list24.getTree());
							}
							break;

					}

					C_SQR_BRACKET25=(Token)match(input,C_SQR_BRACKET,FOLLOW_C_SQR_BRACKET_in_attr_list583); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_C_SQR_BRACKET.add(C_SQR_BRACKET25);

					}
					break;

				default :
					break loop9;
				}
			}

			// AST REWRITE
			// elements: a_list
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 141:6: -> ^( ATTR_LIST ( a_list )* )
			{
				// Dot.g:141:9: ^( ATTR_LIST ( a_list )* )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ATTR_LIST, "ATTR_LIST"), root_1);
				// Dot.g:141:21: ( a_list )*
				while ( stream_a_list.hasNext() ) {
					adaptor.addChild(root_1, stream_a_list.nextTree());
				}
				stream_a_list.reset();

				adaptor.addChild(root_0, root_1);
				}

			}


			retval.tree = root_0;
			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 6, attr_list_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "attr_list"


	public static class a_list_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "a_list"
	// Dot.g:144:1: a_list : ( attr ( COMMA !)? )+ ;
	public final DotParser.a_list_return a_list() throws RecognitionException {
		DotParser.a_list_return retval = new DotParser.a_list_return();
		retval.start = input.LT(1);
		int a_list_StartIndex = input.index();

		CommonTree root_0 = null;

		Token COMMA27=null;
		ParserRuleReturnScope attr26 =null;

		CommonTree COMMA27_tree=null;

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 7) ) { return retval; }

			// Dot.g:145:5: ( ( attr ( COMMA !)? )+ )
			// Dot.g:145:8: ( attr ( COMMA !)? )+
			{
			root_0 = (CommonTree)adaptor.nil();


			// Dot.g:145:8: ( attr ( COMMA !)? )+
			int cnt11=0;
			loop11:
			while (true) {
				int alt11=2;
				int LA11_0 = input.LA(1);
				if ( (LA11_0==ID) ) {
					alt11=1;
				}

				switch (alt11) {
				case 1 :
					// Dot.g:145:9: attr ( COMMA !)?
					{
					pushFollow(FOLLOW_attr_in_a_list618);
					attr26=attr();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, attr26.getTree());

					// Dot.g:145:19: ( COMMA !)?
					int alt10=2;
					int LA10_0 = input.LA(1);
					if ( (LA10_0==COMMA) ) {
						alt10=1;
					}
					switch (alt10) {
						case 1 :
							// Dot.g:145:19: COMMA !
							{
							COMMA27=(Token)match(input,COMMA,FOLLOW_COMMA_in_a_list620); if (state.failed) return retval;
							}
							break;

					}

					}
					break;

				default :
					if ( cnt11 >= 1 ) break loop11;
					if (state.backtracking>0) {state.failed=true; return retval;}
					EarlyExitException eee = new EarlyExitException(11, input);
					throw eee;
				}
				cnt11++;
			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 7, a_list_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "a_list"


	public static class attr_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "attr"
	// Dot.g:148:1: attr : ID ( EQUAL ID )? -> ^( ATTR ID ( EQUAL ID )? ) ;
	public final DotParser.attr_return attr() throws RecognitionException {
		DotParser.attr_return retval = new DotParser.attr_return();
		retval.start = input.LT(1);
		int attr_StartIndex = input.index();

		CommonTree root_0 = null;

		Token ID28=null;
		Token EQUAL29=null;
		Token ID30=null;

		CommonTree ID28_tree=null;
		CommonTree EQUAL29_tree=null;
		CommonTree ID30_tree=null;
		RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
		RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 8) ) { return retval; }

			// Dot.g:149:2: ( ID ( EQUAL ID )? -> ^( ATTR ID ( EQUAL ID )? ) )
			// Dot.g:150:3: ID ( EQUAL ID )?
			{
			ID28=(Token)match(input,ID,FOLLOW_ID_in_attr645); if (state.failed) return retval; 
			if ( state.backtracking==0 ) stream_ID.add(ID28);

			// Dot.g:150:6: ( EQUAL ID )?
			int alt12=2;
			int LA12_0 = input.LA(1);
			if ( (LA12_0==EQUAL) ) {
				alt12=1;
			}
			switch (alt12) {
				case 1 :
					// Dot.g:150:7: EQUAL ID
					{
					EQUAL29=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_attr648); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_EQUAL.add(EQUAL29);

					ID30=(Token)match(input,ID,FOLLOW_ID_in_attr650); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_ID.add(ID30);

					}
					break;

			}

			// AST REWRITE
			// elements: EQUAL, ID, ID
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 150:18: -> ^( ATTR ID ( EQUAL ID )? )
			{
				// Dot.g:150:21: ^( ATTR ID ( EQUAL ID )? )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ATTR, "ATTR"), root_1);
				adaptor.addChild(root_1, stream_ID.nextNode());
				// Dot.g:150:31: ( EQUAL ID )?
				if ( stream_EQUAL.hasNext()||stream_ID.hasNext() ) {
					adaptor.addChild(root_1, stream_EQUAL.nextNode());
					adaptor.addChild(root_1, stream_ID.nextNode());
				}
				stream_EQUAL.reset();
				stream_ID.reset();

				adaptor.addChild(root_0, root_1);
				}

			}


			retval.tree = root_0;
			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 8, attr_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "attr"


	public static class edge_stmt_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "edge_stmt"
	// Dot.g:153:1: edge_stmt : node_subgraph edgeRHS ( attr_list )? -> ^( EDGE_STMT node_subgraph edgeRHS ( attr_list )? ) ;
	public final DotParser.edge_stmt_return edge_stmt() throws RecognitionException {
		DotParser.edge_stmt_return retval = new DotParser.edge_stmt_return();
		retval.start = input.LT(1);
		int edge_stmt_StartIndex = input.index();

		CommonTree root_0 = null;

		ParserRuleReturnScope node_subgraph31 =null;
		ParserRuleReturnScope edgeRHS32 =null;
		ParserRuleReturnScope attr_list33 =null;

		RewriteRuleSubtreeStream stream_node_subgraph=new RewriteRuleSubtreeStream(adaptor,"rule node_subgraph");
		RewriteRuleSubtreeStream stream_edgeRHS=new RewriteRuleSubtreeStream(adaptor,"rule edgeRHS");
		RewriteRuleSubtreeStream stream_attr_list=new RewriteRuleSubtreeStream(adaptor,"rule attr_list");

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 9) ) { return retval; }

			// Dot.g:154:5: ( node_subgraph edgeRHS ( attr_list )? -> ^( EDGE_STMT node_subgraph edgeRHS ( attr_list )? ) )
			// Dot.g:155:7: node_subgraph edgeRHS ( attr_list )?
			{
			pushFollow(FOLLOW_node_subgraph_in_edge_stmt689);
			node_subgraph31=node_subgraph();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_node_subgraph.add(node_subgraph31.getTree());
			pushFollow(FOLLOW_edgeRHS_in_edge_stmt691);
			edgeRHS32=edgeRHS();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_edgeRHS.add(edgeRHS32.getTree());
			// Dot.g:155:29: ( attr_list )?
			int alt13=2;
			switch ( input.LA(1) ) {
				case O_SQR_BRACKET:
					{
					alt13=1;
					}
					break;
				case SEMI_COLON:
					{
					int LA13_2 = input.LA(2);
					if ( (synpred18_Dot()) ) {
						alt13=1;
					}
					}
					break;
				case GRAPH:
					{
					int LA13_3 = input.LA(2);
					if ( (synpred18_Dot()) ) {
						alt13=1;
					}
					}
					break;
				case NODE:
					{
					int LA13_4 = input.LA(2);
					if ( (synpred18_Dot()) ) {
						alt13=1;
					}
					}
					break;
				case EDGE:
					{
					int LA13_5 = input.LA(2);
					if ( (synpred18_Dot()) ) {
						alt13=1;
					}
					}
					break;
				case ID:
					{
					int LA13_6 = input.LA(2);
					if ( (synpred18_Dot()) ) {
						alt13=1;
					}
					}
					break;
				case O_BRACKET:
					{
					int LA13_7 = input.LA(2);
					if ( (synpred18_Dot()) ) {
						alt13=1;
					}
					}
					break;
				case SUBGRAPH:
					{
					int LA13_8 = input.LA(2);
					if ( (synpred18_Dot()) ) {
						alt13=1;
					}
					}
					break;
				case C_BRACKET:
					{
					int LA13_9 = input.LA(2);
					if ( (synpred18_Dot()) ) {
						alt13=1;
					}
					}
					break;
				case EOF:
					{
					int LA13_10 = input.LA(2);
					if ( (synpred18_Dot()) ) {
						alt13=1;
					}
					}
					break;
			}
			switch (alt13) {
				case 1 :
					// Dot.g:155:29: attr_list
					{
					pushFollow(FOLLOW_attr_list_in_edge_stmt693);
					attr_list33=attr_list();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_attr_list.add(attr_list33.getTree());
					}
					break;

			}

			// AST REWRITE
			// elements: attr_list, edgeRHS, node_subgraph
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 156:6: -> ^( EDGE_STMT node_subgraph edgeRHS ( attr_list )? )
			{
				// Dot.g:156:9: ^( EDGE_STMT node_subgraph edgeRHS ( attr_list )? )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(EDGE_STMT, "EDGE_STMT"), root_1);
				adaptor.addChild(root_1, stream_node_subgraph.nextTree());
				adaptor.addChild(root_1, stream_edgeRHS.nextTree());
				// Dot.g:156:43: ( attr_list )?
				if ( stream_attr_list.hasNext() ) {
					adaptor.addChild(root_1, stream_attr_list.nextTree());
				}
				stream_attr_list.reset();

				adaptor.addChild(root_0, root_1);
				}

			}


			retval.tree = root_0;
			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 9, edge_stmt_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "edge_stmt"


	public static class node_subgraph_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "node_subgraph"
	// Dot.g:159:1: node_subgraph : ( node_id | subgraph ) ;
	public final DotParser.node_subgraph_return node_subgraph() throws RecognitionException {
		DotParser.node_subgraph_return retval = new DotParser.node_subgraph_return();
		retval.start = input.LT(1);
		int node_subgraph_StartIndex = input.index();

		CommonTree root_0 = null;

		ParserRuleReturnScope node_id34 =null;
		ParserRuleReturnScope subgraph35 =null;


		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 10) ) { return retval; }

			// Dot.g:160:2: ( ( node_id | subgraph ) )
			// Dot.g:161:2: ( node_id | subgraph )
			{
			root_0 = (CommonTree)adaptor.nil();


			// Dot.g:161:2: ( node_id | subgraph )
			int alt14=2;
			int LA14_0 = input.LA(1);
			if ( (LA14_0==ID) ) {
				alt14=1;
			}
			else if ( (LA14_0==O_BRACKET||LA14_0==SUBGRAPH) ) {
				alt14=2;
			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 14, 0, input);
				throw nvae;
			}

			switch (alt14) {
				case 1 :
					// Dot.g:161:3: node_id
					{
					pushFollow(FOLLOW_node_id_in_node_subgraph733);
					node_id34=node_id();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, node_id34.getTree());

					}
					break;
				case 2 :
					// Dot.g:161:13: subgraph
					{
					pushFollow(FOLLOW_subgraph_in_node_subgraph737);
					subgraph35=subgraph();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, subgraph35.getTree());

					}
					break;

			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 10, node_subgraph_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "node_subgraph"


	public static class edgeRHS_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "edgeRHS"
	// Dot.g:164:1: edgeRHS : EDGEOP ^ ( node_id | subgraph ) ( edgeRHS )? ;
	public final DotParser.edgeRHS_return edgeRHS() throws RecognitionException {
		DotParser.edgeRHS_return retval = new DotParser.edgeRHS_return();
		retval.start = input.LT(1);
		int edgeRHS_StartIndex = input.index();

		CommonTree root_0 = null;

		Token EDGEOP36=null;
		ParserRuleReturnScope node_id37 =null;
		ParserRuleReturnScope subgraph38 =null;
		ParserRuleReturnScope edgeRHS39 =null;

		CommonTree EDGEOP36_tree=null;

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 11) ) { return retval; }

			// Dot.g:165:5: ( EDGEOP ^ ( node_id | subgraph ) ( edgeRHS )? )
			// Dot.g:165:8: EDGEOP ^ ( node_id | subgraph ) ( edgeRHS )?
			{
			root_0 = (CommonTree)adaptor.nil();


			EDGEOP36=(Token)match(input,EDGEOP,FOLLOW_EDGEOP_in_edgeRHS757); if (state.failed) return retval;
			if ( state.backtracking==0 ) {
			EDGEOP36_tree = (CommonTree)adaptor.create(EDGEOP36);
			root_0 = (CommonTree)adaptor.becomeRoot(EDGEOP36_tree, root_0);
			}

			// Dot.g:165:16: ( node_id | subgraph )
			int alt15=2;
			int LA15_0 = input.LA(1);
			if ( (LA15_0==ID) ) {
				alt15=1;
			}
			else if ( (LA15_0==O_BRACKET||LA15_0==SUBGRAPH) ) {
				alt15=2;
			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 15, 0, input);
				throw nvae;
			}

			switch (alt15) {
				case 1 :
					// Dot.g:165:17: node_id
					{
					pushFollow(FOLLOW_node_id_in_edgeRHS761);
					node_id37=node_id();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, node_id37.getTree());

					}
					break;
				case 2 :
					// Dot.g:165:27: subgraph
					{
					pushFollow(FOLLOW_subgraph_in_edgeRHS765);
					subgraph38=subgraph();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, subgraph38.getTree());

					}
					break;

			}

			// Dot.g:165:37: ( edgeRHS )?
			int alt16=2;
			int LA16_0 = input.LA(1);
			if ( (LA16_0==EDGEOP) ) {
				alt16=1;
			}
			switch (alt16) {
				case 1 :
					// Dot.g:165:38: edgeRHS
					{
					pushFollow(FOLLOW_edgeRHS_in_edgeRHS769);
					edgeRHS39=edgeRHS();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, edgeRHS39.getTree());

					}
					break;

			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 11, edgeRHS_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "edgeRHS"


	public static class node_stmt_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "node_stmt"
	// Dot.g:168:1: node_stmt : node_id ( attr_list )? -> ^( NODE_STMT node_id ( attr_list )? ) ;
	public final DotParser.node_stmt_return node_stmt() throws RecognitionException {
		DotParser.node_stmt_return retval = new DotParser.node_stmt_return();
		retval.start = input.LT(1);
		int node_stmt_StartIndex = input.index();

		CommonTree root_0 = null;

		ParserRuleReturnScope node_id40 =null;
		ParserRuleReturnScope attr_list41 =null;

		RewriteRuleSubtreeStream stream_attr_list=new RewriteRuleSubtreeStream(adaptor,"rule attr_list");
		RewriteRuleSubtreeStream stream_node_id=new RewriteRuleSubtreeStream(adaptor,"rule node_id");

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 12) ) { return retval; }

			// Dot.g:169:5: ( node_id ( attr_list )? -> ^( NODE_STMT node_id ( attr_list )? ) )
			// Dot.g:169:8: node_id ( attr_list )?
			{
			pushFollow(FOLLOW_node_id_in_node_stmt793);
			node_id40=node_id();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) stream_node_id.add(node_id40.getTree());
			// Dot.g:169:16: ( attr_list )?
			int alt17=2;
			switch ( input.LA(1) ) {
				case O_SQR_BRACKET:
					{
					alt17=1;
					}
					break;
				case SEMI_COLON:
					{
					int LA17_2 = input.LA(2);
					if ( (synpred22_Dot()) ) {
						alt17=1;
					}
					}
					break;
				case GRAPH:
					{
					int LA17_3 = input.LA(2);
					if ( (synpred22_Dot()) ) {
						alt17=1;
					}
					}
					break;
				case NODE:
					{
					int LA17_4 = input.LA(2);
					if ( (synpred22_Dot()) ) {
						alt17=1;
					}
					}
					break;
				case EDGE:
					{
					int LA17_5 = input.LA(2);
					if ( (synpred22_Dot()) ) {
						alt17=1;
					}
					}
					break;
				case ID:
					{
					int LA17_6 = input.LA(2);
					if ( (synpred22_Dot()) ) {
						alt17=1;
					}
					}
					break;
				case O_BRACKET:
					{
					int LA17_7 = input.LA(2);
					if ( (synpred22_Dot()) ) {
						alt17=1;
					}
					}
					break;
				case SUBGRAPH:
					{
					int LA17_8 = input.LA(2);
					if ( (synpred22_Dot()) ) {
						alt17=1;
					}
					}
					break;
				case C_BRACKET:
					{
					int LA17_9 = input.LA(2);
					if ( (synpred22_Dot()) ) {
						alt17=1;
					}
					}
					break;
				case EOF:
					{
					int LA17_10 = input.LA(2);
					if ( (synpred22_Dot()) ) {
						alt17=1;
					}
					}
					break;
			}
			switch (alt17) {
				case 1 :
					// Dot.g:169:17: attr_list
					{
					pushFollow(FOLLOW_attr_list_in_node_stmt796);
					attr_list41=attr_list();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) stream_attr_list.add(attr_list41.getTree());
					}
					break;

			}

			// AST REWRITE
			// elements: attr_list, node_id
			// token labels: 
			// rule labels: retval
			// token list labels: 
			// rule list labels: 
			// wildcard labels: 
			if ( state.backtracking==0 ) {
			retval.tree = root_0;
			RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

			root_0 = (CommonTree)adaptor.nil();
			// 169:29: -> ^( NODE_STMT node_id ( attr_list )? )
			{
				// Dot.g:169:32: ^( NODE_STMT node_id ( attr_list )? )
				{
				CommonTree root_1 = (CommonTree)adaptor.nil();
				root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_STMT, "NODE_STMT"), root_1);
				adaptor.addChild(root_1, stream_node_id.nextTree());
				// Dot.g:169:52: ( attr_list )?
				if ( stream_attr_list.hasNext() ) {
					adaptor.addChild(root_1, stream_attr_list.nextTree());
				}
				stream_attr_list.reset();

				adaptor.addChild(root_0, root_1);
				}

			}


			retval.tree = root_0;
			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 12, node_stmt_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "node_stmt"


	public static class node_id_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "node_id"
	// Dot.g:172:1: node_id : ID ^ ( port )? ;
	public final DotParser.node_id_return node_id() throws RecognitionException {
		DotParser.node_id_return retval = new DotParser.node_id_return();
		retval.start = input.LT(1);
		int node_id_StartIndex = input.index();

		CommonTree root_0 = null;

		Token ID42=null;
		ParserRuleReturnScope port43 =null;

		CommonTree ID42_tree=null;

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 13) ) { return retval; }

			// Dot.g:173:5: ( ID ^ ( port )? )
			// Dot.g:173:8: ID ^ ( port )?
			{
			root_0 = (CommonTree)adaptor.nil();


			ID42=(Token)match(input,ID,FOLLOW_ID_in_node_id831); if (state.failed) return retval;
			if ( state.backtracking==0 ) {
			ID42_tree = (CommonTree)adaptor.create(ID42);
			root_0 = (CommonTree)adaptor.becomeRoot(ID42_tree, root_0);
			}

			// Dot.g:173:12: ( port )?
			int alt18=2;
			int LA18_0 = input.LA(1);
			if ( (LA18_0==COLON) ) {
				alt18=1;
			}
			switch (alt18) {
				case 1 :
					// Dot.g:173:13: port
					{
					pushFollow(FOLLOW_port_in_node_id835);
					port43=port();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) adaptor.addChild(root_0, port43.getTree());

					}
					break;

			}

			}

			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 13, node_id_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "node_id"


	public static class port_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "port"
	// Dot.g:176:1: port : ( COLON ! ID ( COLON ! VALIDSTR )? | COLON ! VALIDSTR );
	public final DotParser.port_return port() throws RecognitionException {
		DotParser.port_return retval = new DotParser.port_return();
		retval.start = input.LT(1);
		int port_StartIndex = input.index();

		CommonTree root_0 = null;

		Token COLON44=null;
		Token ID45=null;
		Token COLON46=null;
		Token VALIDSTR47=null;
		Token COLON48=null;
		Token VALIDSTR49=null;

		CommonTree COLON44_tree=null;
		CommonTree ID45_tree=null;
		CommonTree COLON46_tree=null;
		CommonTree VALIDSTR47_tree=null;
		CommonTree COLON48_tree=null;
		CommonTree VALIDSTR49_tree=null;

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 14) ) { return retval; }

			// Dot.g:177:5: ( COLON ! ID ( COLON ! VALIDSTR )? | COLON ! VALIDSTR )
			int alt20=2;
			int LA20_0 = input.LA(1);
			if ( (LA20_0==COLON) ) {
				int LA20_1 = input.LA(2);
				if ( (LA20_1==ID) ) {
					alt20=1;
				}
				else if ( (LA20_1==VALIDSTR) ) {
					alt20=2;
				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 20, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 20, 0, input);
				throw nvae;
			}

			switch (alt20) {
				case 1 :
					// Dot.g:178:6: COLON ! ID ( COLON ! VALIDSTR )?
					{
					root_0 = (CommonTree)adaptor.nil();


					COLON44=(Token)match(input,COLON,FOLLOW_COLON_in_port860); if (state.failed) return retval;
					ID45=(Token)match(input,ID,FOLLOW_ID_in_port863); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					ID45_tree = (CommonTree)adaptor.create(ID45);
					adaptor.addChild(root_0, ID45_tree);
					}

					// Dot.g:178:16: ( COLON ! VALIDSTR )?
					int alt19=2;
					int LA19_0 = input.LA(1);
					if ( (LA19_0==COLON) ) {
						alt19=1;
					}
					switch (alt19) {
						case 1 :
							// Dot.g:178:17: COLON ! VALIDSTR
							{
							COLON46=(Token)match(input,COLON,FOLLOW_COLON_in_port866); if (state.failed) return retval;
							VALIDSTR47=(Token)match(input,VALIDSTR,FOLLOW_VALIDSTR_in_port869); if (state.failed) return retval;
							if ( state.backtracking==0 ) {
							VALIDSTR47_tree = (CommonTree)adaptor.create(VALIDSTR47);
							adaptor.addChild(root_0, VALIDSTR47_tree);
							}

							}
							break;

					}

					}
					break;
				case 2 :
					// Dot.g:179:6: COLON ! VALIDSTR
					{
					root_0 = (CommonTree)adaptor.nil();


					COLON48=(Token)match(input,COLON,FOLLOW_COLON_in_port880); if (state.failed) return retval;
					VALIDSTR49=(Token)match(input,VALIDSTR,FOLLOW_VALIDSTR_in_port883); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					VALIDSTR49_tree = (CommonTree)adaptor.create(VALIDSTR49);
					adaptor.addChild(root_0, VALIDSTR49_tree);
					}

					}
					break;

			}
			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 14, port_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "port"


	public static class subgraph_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "subgraph"
	// Dot.g:182:1: subgraph : ( O_BRACKET ( stmt_list )? C_BRACKET -> ^( SUBGRAPH_ROOT ( stmt_list )? ) | ( SUBGRAPH O_BRACKET )=> SUBGRAPH ( ID )? O_BRACKET ( stmt_list )? C_BRACKET -> ^( SUBGRAPH_ROOT ( ID )? ( stmt_list )? ) | SUBGRAPH ( ID )? O_BRACKET ( stmt_list )? C_BRACKET -> ^( SUBGRAPH_ROOT ( ID )? ( stmt_list )? ) | SUBGRAPH ID -> ^( SUBGRAPH_ROOT ID ) );
	public final DotParser.subgraph_return subgraph() throws RecognitionException {
		DotParser.subgraph_return retval = new DotParser.subgraph_return();
		retval.start = input.LT(1);
		int subgraph_StartIndex = input.index();

		CommonTree root_0 = null;

		Token O_BRACKET50=null;
		Token C_BRACKET52=null;
		Token SUBGRAPH53=null;
		Token ID54=null;
		Token O_BRACKET55=null;
		Token C_BRACKET57=null;
		Token SUBGRAPH58=null;
		Token ID59=null;
		Token O_BRACKET60=null;
		Token C_BRACKET62=null;
		Token SUBGRAPH63=null;
		Token ID64=null;
		ParserRuleReturnScope stmt_list51 =null;
		ParserRuleReturnScope stmt_list56 =null;
		ParserRuleReturnScope stmt_list61 =null;

		CommonTree O_BRACKET50_tree=null;
		CommonTree C_BRACKET52_tree=null;
		CommonTree SUBGRAPH53_tree=null;
		CommonTree ID54_tree=null;
		CommonTree O_BRACKET55_tree=null;
		CommonTree C_BRACKET57_tree=null;
		CommonTree SUBGRAPH58_tree=null;
		CommonTree ID59_tree=null;
		CommonTree O_BRACKET60_tree=null;
		CommonTree C_BRACKET62_tree=null;
		CommonTree SUBGRAPH63_tree=null;
		CommonTree ID64_tree=null;
		RewriteRuleTokenStream stream_C_BRACKET=new RewriteRuleTokenStream(adaptor,"token C_BRACKET");
		RewriteRuleTokenStream stream_SUBGRAPH=new RewriteRuleTokenStream(adaptor,"token SUBGRAPH");
		RewriteRuleTokenStream stream_O_BRACKET=new RewriteRuleTokenStream(adaptor,"token O_BRACKET");
		RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
		RewriteRuleSubtreeStream stream_stmt_list=new RewriteRuleSubtreeStream(adaptor,"rule stmt_list");

		try {
			if ( state.backtracking>0 && alreadyParsedRule(input, 15) ) { return retval; }

			// Dot.g:183:5: ( O_BRACKET ( stmt_list )? C_BRACKET -> ^( SUBGRAPH_ROOT ( stmt_list )? ) | ( SUBGRAPH O_BRACKET )=> SUBGRAPH ( ID )? O_BRACKET ( stmt_list )? C_BRACKET -> ^( SUBGRAPH_ROOT ( ID )? ( stmt_list )? ) | SUBGRAPH ( ID )? O_BRACKET ( stmt_list )? C_BRACKET -> ^( SUBGRAPH_ROOT ( ID )? ( stmt_list )? ) | SUBGRAPH ID -> ^( SUBGRAPH_ROOT ID ) )
			int alt26=4;
			int LA26_0 = input.LA(1);
			if ( (LA26_0==O_BRACKET) ) {
				alt26=1;
			}
			else if ( (LA26_0==SUBGRAPH) ) {
				int LA26_2 = input.LA(2);
				if ( (synpred28_Dot()) ) {
					alt26=2;
				}
				else if ( (synpred33_Dot()) ) {
					alt26=3;
				}
				else if ( (true) ) {
					alt26=4;
				}

			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 26, 0, input);
				throw nvae;
			}

			switch (alt26) {
				case 1 :
					// Dot.g:184:6: O_BRACKET ( stmt_list )? C_BRACKET
					{
					O_BRACKET50=(Token)match(input,O_BRACKET,FOLLOW_O_BRACKET_in_subgraph907); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_O_BRACKET.add(O_BRACKET50);

					// Dot.g:184:16: ( stmt_list )?
					int alt21=2;
					int LA21_0 = input.LA(1);
					if ( (LA21_0==EDGE||LA21_0==GRAPH||LA21_0==ID||LA21_0==NODE||LA21_0==O_BRACKET||LA21_0==SUBGRAPH) ) {
						alt21=1;
					}
					switch (alt21) {
						case 1 :
							// Dot.g:184:16: stmt_list
							{
							pushFollow(FOLLOW_stmt_list_in_subgraph909);
							stmt_list51=stmt_list();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_stmt_list.add(stmt_list51.getTree());
							}
							break;

					}

					C_BRACKET52=(Token)match(input,C_BRACKET,FOLLOW_C_BRACKET_in_subgraph912); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_C_BRACKET.add(C_BRACKET52);

					// AST REWRITE
					// elements: stmt_list
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 185:7: -> ^( SUBGRAPH_ROOT ( stmt_list )? )
					{
						// Dot.g:185:10: ^( SUBGRAPH_ROOT ( stmt_list )? )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SUBGRAPH_ROOT, "SUBGRAPH_ROOT"), root_1);
						// Dot.g:185:26: ( stmt_list )?
						if ( stream_stmt_list.hasNext() ) {
							adaptor.addChild(root_1, stream_stmt_list.nextTree());
						}
						stream_stmt_list.reset();

						adaptor.addChild(root_0, root_1);
						}

					}


					retval.tree = root_0;
					}

					}
					break;
				case 2 :
					// Dot.g:187:7: ( SUBGRAPH O_BRACKET )=> SUBGRAPH ( ID )? O_BRACKET ( stmt_list )? C_BRACKET
					{
					SUBGRAPH53=(Token)match(input,SUBGRAPH,FOLLOW_SUBGRAPH_in_subgraph958); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_SUBGRAPH.add(SUBGRAPH53);

					// Dot.g:188:16: ( ID )?
					int alt22=2;
					int LA22_0 = input.LA(1);
					if ( (LA22_0==ID) ) {
						alt22=1;
					}
					switch (alt22) {
						case 1 :
							// Dot.g:188:16: ID
							{
							ID54=(Token)match(input,ID,FOLLOW_ID_in_subgraph960); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_ID.add(ID54);

							}
							break;

					}

					O_BRACKET55=(Token)match(input,O_BRACKET,FOLLOW_O_BRACKET_in_subgraph963); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_O_BRACKET.add(O_BRACKET55);

					// Dot.g:188:30: ( stmt_list )?
					int alt23=2;
					int LA23_0 = input.LA(1);
					if ( (LA23_0==EDGE||LA23_0==GRAPH||LA23_0==ID||LA23_0==NODE||LA23_0==O_BRACKET||LA23_0==SUBGRAPH) ) {
						alt23=1;
					}
					switch (alt23) {
						case 1 :
							// Dot.g:188:30: stmt_list
							{
							pushFollow(FOLLOW_stmt_list_in_subgraph965);
							stmt_list56=stmt_list();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_stmt_list.add(stmt_list56.getTree());
							}
							break;

					}

					C_BRACKET57=(Token)match(input,C_BRACKET,FOLLOW_C_BRACKET_in_subgraph968); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_C_BRACKET.add(C_BRACKET57);

					// AST REWRITE
					// elements: ID, stmt_list
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 189:7: -> ^( SUBGRAPH_ROOT ( ID )? ( stmt_list )? )
					{
						// Dot.g:189:10: ^( SUBGRAPH_ROOT ( ID )? ( stmt_list )? )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SUBGRAPH_ROOT, "SUBGRAPH_ROOT"), root_1);
						// Dot.g:189:26: ( ID )?
						if ( stream_ID.hasNext() ) {
							adaptor.addChild(root_1, stream_ID.nextNode());
						}
						stream_ID.reset();

						// Dot.g:189:30: ( stmt_list )?
						if ( stream_stmt_list.hasNext() ) {
							adaptor.addChild(root_1, stream_stmt_list.nextTree());
						}
						stream_stmt_list.reset();

						adaptor.addChild(root_0, root_1);
						}

					}


					retval.tree = root_0;
					}

					}
					break;
				case 3 :
					// Dot.g:191:8: SUBGRAPH ( ID )? O_BRACKET ( stmt_list )? C_BRACKET
					{
					SUBGRAPH58=(Token)match(input,SUBGRAPH,FOLLOW_SUBGRAPH_in_subgraph1004); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_SUBGRAPH.add(SUBGRAPH58);

					// Dot.g:191:17: ( ID )?
					int alt24=2;
					int LA24_0 = input.LA(1);
					if ( (LA24_0==ID) ) {
						alt24=1;
					}
					switch (alt24) {
						case 1 :
							// Dot.g:191:17: ID
							{
							ID59=(Token)match(input,ID,FOLLOW_ID_in_subgraph1006); if (state.failed) return retval; 
							if ( state.backtracking==0 ) stream_ID.add(ID59);

							}
							break;

					}

					O_BRACKET60=(Token)match(input,O_BRACKET,FOLLOW_O_BRACKET_in_subgraph1009); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_O_BRACKET.add(O_BRACKET60);

					// Dot.g:191:31: ( stmt_list )?
					int alt25=2;
					int LA25_0 = input.LA(1);
					if ( (LA25_0==EDGE||LA25_0==GRAPH||LA25_0==ID||LA25_0==NODE||LA25_0==O_BRACKET||LA25_0==SUBGRAPH) ) {
						alt25=1;
					}
					switch (alt25) {
						case 1 :
							// Dot.g:191:31: stmt_list
							{
							pushFollow(FOLLOW_stmt_list_in_subgraph1011);
							stmt_list61=stmt_list();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) stream_stmt_list.add(stmt_list61.getTree());
							}
							break;

					}

					C_BRACKET62=(Token)match(input,C_BRACKET,FOLLOW_C_BRACKET_in_subgraph1014); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_C_BRACKET.add(C_BRACKET62);

					// AST REWRITE
					// elements: stmt_list, ID
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 192:7: -> ^( SUBGRAPH_ROOT ( ID )? ( stmt_list )? )
					{
						// Dot.g:192:10: ^( SUBGRAPH_ROOT ( ID )? ( stmt_list )? )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SUBGRAPH_ROOT, "SUBGRAPH_ROOT"), root_1);
						// Dot.g:192:26: ( ID )?
						if ( stream_ID.hasNext() ) {
							adaptor.addChild(root_1, stream_ID.nextNode());
						}
						stream_ID.reset();

						// Dot.g:192:30: ( stmt_list )?
						if ( stream_stmt_list.hasNext() ) {
							adaptor.addChild(root_1, stream_stmt_list.nextTree());
						}
						stream_stmt_list.reset();

						adaptor.addChild(root_0, root_1);
						}

					}


					retval.tree = root_0;
					}

					}
					break;
				case 4 :
					// Dot.g:194:6: SUBGRAPH ID
					{
					SUBGRAPH63=(Token)match(input,SUBGRAPH,FOLLOW_SUBGRAPH_in_subgraph1048); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_SUBGRAPH.add(SUBGRAPH63);

					ID64=(Token)match(input,ID,FOLLOW_ID_in_subgraph1050); if (state.failed) return retval; 
					if ( state.backtracking==0 ) stream_ID.add(ID64);

					// AST REWRITE
					// elements: ID
					// token labels: 
					// rule labels: retval
					// token list labels: 
					// rule list labels: 
					// wildcard labels: 
					if ( state.backtracking==0 ) {
					retval.tree = root_0;
					RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.getTree():null);

					root_0 = (CommonTree)adaptor.nil();
					// 195:7: -> ^( SUBGRAPH_ROOT ID )
					{
						// Dot.g:195:10: ^( SUBGRAPH_ROOT ID )
						{
						CommonTree root_1 = (CommonTree)adaptor.nil();
						root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SUBGRAPH_ROOT, "SUBGRAPH_ROOT"), root_1);
						adaptor.addChild(root_1, stream_ID.nextNode());
						adaptor.addChild(root_0, root_1);
						}

					}


					retval.tree = root_0;
					}

					}
					break;

			}
			retval.stop = input.LT(-1);

			if ( state.backtracking==0 ) {
			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
			if ( state.backtracking>0 ) { memoize(input, 15, subgraph_StartIndex); }

		}
		return retval;
	}
	// $ANTLR end "subgraph"

	// $ANTLR start synpred8_Dot
	public final void synpred8_Dot_fragment() throws RecognitionException {
		// Dot.g:128:9: ( edge_stmt )
		// Dot.g:128:9: edge_stmt
		{
		pushFollow(FOLLOW_edge_stmt_in_synpred8_Dot473);
		edge_stmt();
		state._fsp--;
		if (state.failed) return;

		}

	}
	// $ANTLR end synpred8_Dot

	// $ANTLR start synpred9_Dot
	public final void synpred9_Dot_fragment() throws RecognitionException {
		// Dot.g:129:6: ( subgraph )
		// Dot.g:129:6: subgraph
		{
		pushFollow(FOLLOW_subgraph_in_synpred9_Dot482);
		subgraph();
		state._fsp--;
		if (state.failed) return;

		}

	}
	// $ANTLR end synpred9_Dot

	// $ANTLR start synpred10_Dot
	public final void synpred10_Dot_fragment() throws RecognitionException {
		// Dot.g:130:6: ( ID EQUAL ID )
		// Dot.g:130:6: ID EQUAL ID
		{
		match(input,ID,FOLLOW_ID_in_synpred10_Dot491); if (state.failed) return;

		match(input,EQUAL,FOLLOW_EQUAL_in_synpred10_Dot493); if (state.failed) return;

		match(input,ID,FOLLOW_ID_in_synpred10_Dot495); if (state.failed) return;

		}

	}
	// $ANTLR end synpred10_Dot

	// $ANTLR start synpred18_Dot
	public final void synpred18_Dot_fragment() throws RecognitionException {
		// Dot.g:155:29: ( attr_list )
		// Dot.g:155:29: attr_list
		{
		pushFollow(FOLLOW_attr_list_in_synpred18_Dot693);
		attr_list();
		state._fsp--;
		if (state.failed) return;

		}

	}
	// $ANTLR end synpred18_Dot

	// $ANTLR start synpred22_Dot
	public final void synpred22_Dot_fragment() throws RecognitionException {
		// Dot.g:169:17: ( attr_list )
		// Dot.g:169:17: attr_list
		{
		pushFollow(FOLLOW_attr_list_in_synpred22_Dot796);
		attr_list();
		state._fsp--;
		if (state.failed) return;

		}

	}
	// $ANTLR end synpred22_Dot

	// $ANTLR start synpred28_Dot
	public final void synpred28_Dot_fragment() throws RecognitionException {
		// Dot.g:187:7: ( SUBGRAPH O_BRACKET )
		// Dot.g:187:8: SUBGRAPH O_BRACKET
		{
		match(input,SUBGRAPH,FOLLOW_SUBGRAPH_in_synpred28_Dot944); if (state.failed) return;

		match(input,O_BRACKET,FOLLOW_O_BRACKET_in_synpred28_Dot946); if (state.failed) return;

		}

	}
	// $ANTLR end synpred28_Dot

	// $ANTLR start synpred33_Dot
	public final void synpred33_Dot_fragment() throws RecognitionException {
		// Dot.g:191:8: ( SUBGRAPH ( ID )? O_BRACKET ( stmt_list )? C_BRACKET )
		// Dot.g:191:8: SUBGRAPH ( ID )? O_BRACKET ( stmt_list )? C_BRACKET
		{
		match(input,SUBGRAPH,FOLLOW_SUBGRAPH_in_synpred33_Dot1004); if (state.failed) return;

		// Dot.g:191:17: ( ID )?
		int alt32=2;
		int LA32_0 = input.LA(1);
		if ( (LA32_0==ID) ) {
			alt32=1;
		}
		switch (alt32) {
			case 1 :
				// Dot.g:191:17: ID
				{
				match(input,ID,FOLLOW_ID_in_synpred33_Dot1006); if (state.failed) return;

				}
				break;

		}

		match(input,O_BRACKET,FOLLOW_O_BRACKET_in_synpred33_Dot1009); if (state.failed) return;

		// Dot.g:191:31: ( stmt_list )?
		int alt33=2;
		int LA33_0 = input.LA(1);
		if ( (LA33_0==EDGE||LA33_0==GRAPH||LA33_0==ID||LA33_0==NODE||LA33_0==O_BRACKET||LA33_0==SUBGRAPH) ) {
			alt33=1;
		}
		switch (alt33) {
			case 1 :
				// Dot.g:191:31: stmt_list
				{
				pushFollow(FOLLOW_stmt_list_in_synpred33_Dot1011);
				stmt_list();
				state._fsp--;
				if (state.failed) return;

				}
				break;

		}

		match(input,C_BRACKET,FOLLOW_C_BRACKET_in_synpred33_Dot1014); if (state.failed) return;

		}

	}
	// $ANTLR end synpred33_Dot

	// Delegated rules

	public final boolean synpred33_Dot() {
		state.backtracking++;
		int start = input.mark();
		try {
			synpred33_Dot_fragment(); // can never throw exception
		} catch (RecognitionException re) {
			System.err.println("impossible: "+re);
		}
		boolean success = !state.failed;
		input.rewind(start);
		state.backtracking--;
		state.failed=false;
		return success;
	}
	public final boolean synpred22_Dot() {
		state.backtracking++;
		int start = input.mark();
		try {
			synpred22_Dot_fragment(); // can never throw exception
		} catch (RecognitionException re) {
			System.err.println("impossible: "+re);
		}
		boolean success = !state.failed;
		input.rewind(start);
		state.backtracking--;
		state.failed=false;
		return success;
	}
	public final boolean synpred9_Dot() {
		state.backtracking++;
		int start = input.mark();
		try {
			synpred9_Dot_fragment(); // can never throw exception
		} catch (RecognitionException re) {
			System.err.println("impossible: "+re);
		}
		boolean success = !state.failed;
		input.rewind(start);
		state.backtracking--;
		state.failed=false;
		return success;
	}
	public final boolean synpred10_Dot() {
		state.backtracking++;
		int start = input.mark();
		try {
			synpred10_Dot_fragment(); // can never throw exception
		} catch (RecognitionException re) {
			System.err.println("impossible: "+re);
		}
		boolean success = !state.failed;
		input.rewind(start);
		state.backtracking--;
		state.failed=false;
		return success;
	}
	public final boolean synpred8_Dot() {
		state.backtracking++;
		int start = input.mark();
		try {
			synpred8_Dot_fragment(); // can never throw exception
		} catch (RecognitionException re) {
			System.err.println("impossible: "+re);
		}
		boolean success = !state.failed;
		input.rewind(start);
		state.backtracking--;
		state.failed=false;
		return success;
	}
	public final boolean synpred18_Dot() {
		state.backtracking++;
		int start = input.mark();
		try {
			synpred18_Dot_fragment(); // can never throw exception
		} catch (RecognitionException re) {
			System.err.println("impossible: "+re);
		}
		boolean success = !state.failed;
		input.rewind(start);
		state.backtracking--;
		state.failed=false;
		return success;
	}
	public final boolean synpred28_Dot() {
		state.backtracking++;
		int start = input.mark();
		try {
			synpred28_Dot_fragment(); // can never throw exception
		} catch (RecognitionException re) {
			System.err.println("impossible: "+re);
		}
		boolean success = !state.failed;
		input.rewind(start);
		state.backtracking--;
		state.failed=false;
		return success;
	}


	protected DFA4 dfa4 = new DFA4(this);
	static final String DFA4_eotS =
		"\4\uffff";
	static final String DFA4_eofS =
		"\2\2\2\uffff";
	static final String DFA4_minS =
		"\2\15\2\uffff";
	static final String DFA4_maxS =
		"\2\67\2\uffff";
	static final String DFA4_acceptS =
		"\2\uffff\1\2\1\1";
	static final String DFA4_specialS =
		"\4\uffff}>";
	static final String[] DFA4_transitionS = {
			"\1\2\4\uffff\1\3\6\uffff\1\3\4\uffff\1\3\10\uffff\1\3\3\uffff\1\3\7\uffff"+
			"\1\1\3\uffff\1\3",
			"\1\2\4\uffff\1\3\6\uffff\1\3\4\uffff\1\3\10\uffff\1\3\3\uffff\1\3\7"+
			"\uffff\1\1\3\uffff\1\3",
			"",
			""
	};

	static final short[] DFA4_eot = DFA.unpackEncodedString(DFA4_eotS);
	static final short[] DFA4_eof = DFA.unpackEncodedString(DFA4_eofS);
	static final char[] DFA4_min = DFA.unpackEncodedStringToUnsignedChars(DFA4_minS);
	static final char[] DFA4_max = DFA.unpackEncodedStringToUnsignedChars(DFA4_maxS);
	static final short[] DFA4_accept = DFA.unpackEncodedString(DFA4_acceptS);
	static final short[] DFA4_special = DFA.unpackEncodedString(DFA4_specialS);
	static final short[][] DFA4_transition;

	static {
		int numStates = DFA4_transitionS.length;
		DFA4_transition = new short[numStates][];
		for (int i=0; i<numStates; i++) {
			DFA4_transition[i] = DFA.unpackEncodedString(DFA4_transitionS[i]);
		}
	}

	protected class DFA4 extends DFA {

		public DFA4(BaseRecognizer recognizer) {
			this.recognizer = recognizer;
			this.decisionNumber = 4;
			this.eot = DFA4_eot;
			this.eof = DFA4_eof;
			this.min = DFA4_min;
			this.max = DFA4_max;
			this.accept = DFA4_accept;
			this.special = DFA4_special;
			this.transition = DFA4_transition;
		}
		@Override
		public String getDescription() {
			return "()* loopback of 122:13: ( ( SEMI_COLON )* stmt )*";
		}
	}

	public static final BitSet FOLLOW_graphModifier_in_graph327 = new BitSet(new long[]{0x0000080040000000L});
	public static final BitSet FOLLOW_ID_in_graph329 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_O_BRACKET_in_graph332 = new BitSet(new long[]{0x0080088042040000L});
	public static final BitSet FOLLOW_stmt_list_in_graph334 = new BitSet(new long[]{0x0000000000002000L});
	public static final BitSet FOLLOW_C_BRACKET_in_graph336 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_STRICT_in_graphModifier385 = new BitSet(new long[]{0x0000000002010000L});
	public static final BitSet FOLLOW_set_in_graphModifier389 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_stmt_in_stmt_list414 = new BitSet(new long[]{0x0088088042040002L});
	public static final BitSet FOLLOW_SEMI_COLON_in_stmt_list417 = new BitSet(new long[]{0x0088088042040000L});
	public static final BitSet FOLLOW_stmt_in_stmt_list420 = new BitSet(new long[]{0x0088088042040002L});
	public static final BitSet FOLLOW_SEMI_COLON_in_stmt_list424 = new BitSet(new long[]{0x0008000000000002L});
	public static final BitSet FOLLOW_attr_stmt_in_stmt461 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_edge_stmt_in_stmt473 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_subgraph_in_stmt482 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ID_in_stmt491 = new BitSet(new long[]{0x0000000000200000L});
	public static final BitSet FOLLOW_EQUAL_in_stmt493 = new BitSet(new long[]{0x0000000040000000L});
	public static final BitSet FOLLOW_ID_in_stmt495 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_node_stmt_in_stmt517 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_GRAPH_in_attr_stmt537 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_NODE_in_attr_stmt542 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_EDGE_in_attr_stmt547 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_attr_list_in_attr_stmt552 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_O_SQR_BRACKET_in_attr_list578 = new BitSet(new long[]{0x0000000040004000L});
	public static final BitSet FOLLOW_a_list_in_attr_list580 = new BitSet(new long[]{0x0000000000004000L});
	public static final BitSet FOLLOW_C_SQR_BRACKET_in_attr_list583 = new BitSet(new long[]{0x0000100000000002L});
	public static final BitSet FOLLOW_attr_in_a_list618 = new BitSet(new long[]{0x0000000040000802L});
	public static final BitSet FOLLOW_COMMA_in_a_list620 = new BitSet(new long[]{0x0000000040000002L});
	public static final BitSet FOLLOW_ID_in_attr645 = new BitSet(new long[]{0x0000000000200002L});
	public static final BitSet FOLLOW_EQUAL_in_attr648 = new BitSet(new long[]{0x0000000040000000L});
	public static final BitSet FOLLOW_ID_in_attr650 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_node_subgraph_in_edge_stmt689 = new BitSet(new long[]{0x0000000000080000L});
	public static final BitSet FOLLOW_edgeRHS_in_edge_stmt691 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_attr_list_in_edge_stmt693 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_node_id_in_node_subgraph733 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_subgraph_in_node_subgraph737 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_EDGEOP_in_edgeRHS757 = new BitSet(new long[]{0x0080080040000000L});
	public static final BitSet FOLLOW_node_id_in_edgeRHS761 = new BitSet(new long[]{0x0000000000080002L});
	public static final BitSet FOLLOW_subgraph_in_edgeRHS765 = new BitSet(new long[]{0x0000000000080002L});
	public static final BitSet FOLLOW_edgeRHS_in_edgeRHS769 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_node_id_in_node_stmt793 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_attr_list_in_node_stmt796 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ID_in_node_id831 = new BitSet(new long[]{0x0000000000000402L});
	public static final BitSet FOLLOW_port_in_node_id835 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_COLON_in_port860 = new BitSet(new long[]{0x0000000040000000L});
	public static final BitSet FOLLOW_ID_in_port863 = new BitSet(new long[]{0x0000000000000402L});
	public static final BitSet FOLLOW_COLON_in_port866 = new BitSet(new long[]{0x1000000000000000L});
	public static final BitSet FOLLOW_VALIDSTR_in_port869 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_COLON_in_port880 = new BitSet(new long[]{0x1000000000000000L});
	public static final BitSet FOLLOW_VALIDSTR_in_port883 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_O_BRACKET_in_subgraph907 = new BitSet(new long[]{0x0080088042042000L});
	public static final BitSet FOLLOW_stmt_list_in_subgraph909 = new BitSet(new long[]{0x0000000000002000L});
	public static final BitSet FOLLOW_C_BRACKET_in_subgraph912 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_SUBGRAPH_in_subgraph958 = new BitSet(new long[]{0x0000080040000000L});
	public static final BitSet FOLLOW_ID_in_subgraph960 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_O_BRACKET_in_subgraph963 = new BitSet(new long[]{0x0080088042042000L});
	public static final BitSet FOLLOW_stmt_list_in_subgraph965 = new BitSet(new long[]{0x0000000000002000L});
	public static final BitSet FOLLOW_C_BRACKET_in_subgraph968 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_SUBGRAPH_in_subgraph1004 = new BitSet(new long[]{0x0000080040000000L});
	public static final BitSet FOLLOW_ID_in_subgraph1006 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_O_BRACKET_in_subgraph1009 = new BitSet(new long[]{0x0080088042042000L});
	public static final BitSet FOLLOW_stmt_list_in_subgraph1011 = new BitSet(new long[]{0x0000000000002000L});
	public static final BitSet FOLLOW_C_BRACKET_in_subgraph1014 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_SUBGRAPH_in_subgraph1048 = new BitSet(new long[]{0x0000000040000000L});
	public static final BitSet FOLLOW_ID_in_subgraph1050 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_edge_stmt_in_synpred8_Dot473 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_subgraph_in_synpred9_Dot482 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ID_in_synpred10_Dot491 = new BitSet(new long[]{0x0000000000200000L});
	public static final BitSet FOLLOW_EQUAL_in_synpred10_Dot493 = new BitSet(new long[]{0x0000000040000000L});
	public static final BitSet FOLLOW_ID_in_synpred10_Dot495 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_attr_list_in_synpred18_Dot693 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_attr_list_in_synpred22_Dot796 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_SUBGRAPH_in_synpred28_Dot944 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_O_BRACKET_in_synpred28_Dot946 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_SUBGRAPH_in_synpred33_Dot1004 = new BitSet(new long[]{0x0000080040000000L});
	public static final BitSet FOLLOW_ID_in_synpred33_Dot1006 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_O_BRACKET_in_synpred33_Dot1009 = new BitSet(new long[]{0x0080088042042000L});
	public static final BitSet FOLLOW_stmt_list_in_synpred33_Dot1011 = new BitSet(new long[]{0x0000000000002000L});
	public static final BitSet FOLLOW_C_BRACKET_in_synpred33_Dot1014 = new BitSet(new long[]{0x0000000000000002L});
}
