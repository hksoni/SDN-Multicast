// $ANTLR 3.5.2 DotTree.g 2015-04-28 15:52:39

package org.cesta.parsers.dot;

import java.util.Set;
import java.util.List;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Iterator;

import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;

import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;


/**
 *  An ANTLRv3 capable DOT tree grammar.
 *  Rule "graph" is entry-point, which parses whole AST
 *  and returns graph object.
 *
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
public class DotTree extends TreeParser {
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
	public TreeParser[] getDelegates() {
		return new TreeParser[] {};
	}

	// delegators

	protected static class GraphOrSubgraph_scope {
		Graph obj;
	}
	protected Stack<GraphOrSubgraph_scope> GraphOrSubgraph_stack = new Stack<GraphOrSubgraph_scope>();


	public DotTree(TreeNodeStream input) {
		this(input, new RecognizerSharedState());
	}
	public DotTree(TreeNodeStream input, RecognizerSharedState state) {
		super(input, state);
	}

	protected TreeAdaptor adaptor = new CommonTreeAdaptor();

	public void setTreeAdaptor(TreeAdaptor adaptor) {
		this.adaptor = adaptor;
	}
	public TreeAdaptor getTreeAdaptor() {
		return adaptor;
	}
	@Override public String[] getTokenNames() { return DotTree.tokenNames; }
	@Override public String getGrammarFileName() { return "DotTree.g"; }


	    /**
	     *  Model of graph. Has own attributes and special graph, edge or node attributes,
	     *  nodes (even from subgraphs), edges, subgraphs etc.
	     */
	    public class Graph {

	        public Map<String, String> attributes = new HashMap<String, String>();
	        public Map<String, String> graphAttributes = new HashMap<String, String>();
	        public Map<String, String> edgeAttributes = new HashMap<String, String>();
	        public Map<String, String> nodeAttributes = new HashMap<String, String>();

	        public Map<String, Node> nodes = new HashMap<String, Node>();
	        public List<Edge> edges = new LinkedList<Edge>();
	        public Map<String, SubGraph> subGraphsMap = new HashMap<String, SubGraph>();
	        public Set<SubGraph> subGraphs = new HashSet<SubGraph>();

	        public boolean isStrict = false;
	        public boolean isDirected = false;

	        public String id;
	        public Graph(){

	        }
	        public Graph(String id){
	            this.id = id;
	        }
	        @Override
	        public String toString(){
	            return
	                "Graph"
	                +(id!=null?" "+id:"")
	                +(isStrict?" strict":"")
	                +(isDirected?" directed":"")
	                +","
	                +(!attributes.isEmpty()?" attributes: "+attributes:"")
	                +(!nodes.isEmpty()?" nodes: "+nodes:"")
	            ;
	        }
	        public Set<Node> getNodes(){
	            Set<Node> retNodes = new HashSet<Node>();
	            retNodes.addAll(nodes.values());
	            return retNodes;
	        }
	        public Map<Node, List<Node>> getTransitionMap(){
	            Map<Node, List<Node>> map = new HashMap<Node, List<Node>>();
	            for (Edge e:edges){
	                List<NodePair> pairs = e.getNodePairs();
	                for (NodePair np:pairs){
	                    if (!map.containsKey(np.x))
	                        map.put(np.x, new LinkedList<Node>());
	                    map.get(np.x).add(np.y);
	                }
	            }
	            return map;
	        }
	        public List<NodePair> getNodePairs(){
	            List<NodePair> pairs = new LinkedList<NodePair>();
	            for (Edge e:edges){
	                pairs.addAll(e.getNodePairs());
	            }
	            return pairs;
	        }
	        @Override
	        public boolean equals(Object obj){
	            if (!(obj instanceof Graph)) return false;
	            Graph g = (Graph)obj;
	            return (g.id!=null && g.id.equals(id));
	        }

	    };
	    /**
	     *  Subgraphs - special type of Graph.
	     */
	    public class SubGraph extends Graph {
	        public SubGraph(){
	            super();
	        }
	        public SubGraph(String id){
	            super(id);
	        }
	        @Override
	        public String toString(){
	            return "Sub"+super.toString();
	        }
	    }
	    /**
	     *  Model simple node with id and attributes.
	     */
	    public class Node {
	        public String id;
	        public Map<String, String> attributes = new HashMap<String, String>();
	        public Node(String id){
	            setId(id);
	        }
	        public Node(String id, Map<String, String> attributes){
	            setId(id);
	            this.attributes = attributes;
	        }
	        public void setId(String id){
	            // remove quotes
	            if (id!=null && id.length()>=2 && id.charAt(0)=='"' && id.charAt(id.length()-1)=='"')
	                id = id.substring(1,id.length()-1);
	            this.id = id;
	        }
	        @Override
	        public boolean equals(Object obj){
	            if (!(obj instanceof Node)) return false;
	            Node n = (Node)obj;
	            return (n.id!=null && n.id.equals(id));
	        }
	        @Override
	        public int hashCode() {
	            int hash = 3;
	            hash = 41 * hash + (this.id != null ? this.id.hashCode() : 0);
	            return hash;
	        }
	        @Override
	        public String toString(){
	            return "Node "+id
	                +(!attributes.isEmpty()?" "+attributes:"");
	        }
	    }
	    /**
	     *  Single edge between two nodes.
	     */
	    public class NodePair {
	        public Node x;
	        public Node y;
	        public Map<String, String> attributes;
	        public NodePair(Node x, Node y){
	            this.x = x;
	            this.y = y;
	        }
	        @Override
	        public String toString(){
	            return "["+x+", "+y+"]";
	        }
	    }
	    /**
	     *  Edge, which has attributes and goes through multiple objects (Nodes or SubGraphs)
	     */
	    public class Edge {
	        public Map<String, String> attributes = new HashMap<String, String>();
	        public List<Object> nodes = new LinkedList<Object>();
	        /**
	         *  Returns list of set of nodes, though which the edge goes.
	         */
	        public List<Set<Node>> getEdgeNodes(){
	            List<Set<Node>> edgeNodes = new LinkedList<Set<Node>>();
	            for (Object o:nodes){
	                Set<Node> nodeSet = new HashSet<Node>();

	                if (o instanceof Node) nodeSet.add((Node)o);
	                if (o instanceof SubGraph) {
	                    SubGraph subGraph = (SubGraph) o;
	                    nodeSet.addAll(subGraph.getNodes());
	                }
	                edgeNodes.add(nodeSet);
	            }
	            return edgeNodes;
	        }
	        /**
	         *  Returns list of node pairs (simple edges). It splits full edge to 
	         *  smaller parts. 
	         */
	        public List<NodePair> getNodePairs(){
	            List<NodePair> nodePairs = new LinkedList<NodePair>();
	            List<Set<Node>> edgeNodes = getEdgeNodes();
	            Set<Node> x = null;
	            Set<Node> y = null;
	            Iterator<Set<Node>> it = edgeNodes.iterator();
	            x = it.next();
	            while (it.hasNext()){
	                y = it.next();
	                // create X * Y node pairs
	                for (Node xx:x)
	                    for (Node yy:y){
	                        NodePair np = new NodePair(xx,yy);
	                        np.attributes = this.attributes;
	                        nodePairs.add(np);
	                    }
	                x = y;
	            }
	            return nodePairs;
	        }
	        @Override
	        public String toString(){
	            return "Edge "+attributes+" "+getEdgeNodes();
	        }
	    }


	protected static class graph_scope {
		Graph obj;
	}
	protected Stack<graph_scope> graph_stack = new Stack<graph_scope>();

	public static class graph_return extends TreeRuleReturnScope {
		public Graph graphObj;
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "graph"
	// DotTree.g:257:1: graph returns [Graph graphObj] : ^( GRAPH_ROOT graphModifier ( ID )? stmt_list ) ;
	public final DotTree.graph_return graph() throws RecognitionException {
		GraphOrSubgraph_stack.push(new GraphOrSubgraph_scope());
		graph_stack.push(new graph_scope());
		DotTree.graph_return retval = new DotTree.graph_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree GRAPH_ROOT1=null;
		CommonTree ID3=null;
		TreeRuleReturnScope graphModifier2 =null;
		TreeRuleReturnScope stmt_list4 =null;

		CommonTree GRAPH_ROOT1_tree=null;
		CommonTree ID3_tree=null;


		        GraphOrSubgraph_stack.peek().obj = new Graph();
		        retval.graphObj = GraphOrSubgraph_stack.peek().obj;
		        graph_stack.peek().obj = GraphOrSubgraph_stack.peek().obj;
		    
		try {
			// DotTree.g:267:5: ( ^( GRAPH_ROOT graphModifier ( ID )? stmt_list ) )
			// DotTree.g:268:9: ^( GRAPH_ROOT graphModifier ( ID )? stmt_list )
			{
			root_0 = (CommonTree)adaptor.nil();


			_last = (CommonTree)input.LT(1);
			{
			CommonTree _save_last_1 = _last;
			CommonTree _first_1 = null;
			CommonTree root_1 = (CommonTree)adaptor.nil();
			_last = (CommonTree)input.LT(1);
			GRAPH_ROOT1=(CommonTree)match(input,GRAPH_ROOT,FOLLOW_GRAPH_ROOT_in_graph137); if (state.failed) return retval;
			if ( state.backtracking==0 ) {
			GRAPH_ROOT1_tree = (CommonTree)adaptor.dupNode(GRAPH_ROOT1);


			root_1 = (CommonTree)adaptor.becomeRoot(GRAPH_ROOT1_tree, root_1);
			}

			match(input, Token.DOWN, null); if (state.failed) return retval;
			_last = (CommonTree)input.LT(1);
			pushFollow(FOLLOW_graphModifier_in_graph151);
			graphModifier2=graphModifier();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) 
			adaptor.addChild(root_1, graphModifier2.getTree());

			if ( state.backtracking==0 ) {
			                retval.graphObj.isStrict = (graphModifier2!=null?((DotTree.graphModifier_return)graphModifier2).isStrict:false);
			                retval.graphObj.isDirected = (graphModifier2!=null?((DotTree.graphModifier_return)graphModifier2).isDirected:false);
			            }
			// DotTree.g:274:13: ( ID )?
			int alt1=2;
			int LA1_0 = input.LA(1);
			if ( (LA1_0==ID) ) {
				alt1=1;
			}
			switch (alt1) {
				case 1 :
					// DotTree.g:274:13: ID
					{
					_last = (CommonTree)input.LT(1);
					ID3=(CommonTree)match(input,ID,FOLLOW_ID_in_graph179); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					ID3_tree = (CommonTree)adaptor.dupNode(ID3);


					adaptor.addChild(root_1, ID3_tree);
					}

					if ( state.backtracking==0 ) {
					}

					}
					break;

			}

			if ( state.backtracking==0 ) { if (ID3!=null) retval.graphObj.id = (ID3!=null?ID3.getText():null);}
			_last = (CommonTree)input.LT(1);
			pushFollow(FOLLOW_stmt_list_in_graph196);
			stmt_list4=stmt_list();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) 
			adaptor.addChild(root_1, stmt_list4.getTree());

			match(input, Token.UP, null); if (state.failed) return retval;
			adaptor.addChild(root_0, root_1);
			_last = _save_last_1;
			}


			if ( state.backtracking==0 ) {
			}

			}

			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
			GraphOrSubgraph_stack.pop();
			graph_stack.pop();
		}
		return retval;
	}
	// $ANTLR end "graph"


	public static class graphModifier_return extends TreeRuleReturnScope {
		public boolean isStrict;
		public boolean isDirected;
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "graphModifier"
	// DotTree.g:279:1: graphModifier returns [boolean isStrict, boolean isDirected] : ( STRICT )? ( GRAPH | DIGRAPH ) ;
	public final DotTree.graphModifier_return graphModifier() throws RecognitionException {
		DotTree.graphModifier_return retval = new DotTree.graphModifier_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree STRICT5=null;
		CommonTree GRAPH6=null;
		CommonTree DIGRAPH7=null;

		CommonTree STRICT5_tree=null;
		CommonTree GRAPH6_tree=null;
		CommonTree DIGRAPH7_tree=null;

		try {
			// DotTree.g:280:5: ( ( STRICT )? ( GRAPH | DIGRAPH ) )
			// DotTree.g:281:9: ( STRICT )? ( GRAPH | DIGRAPH )
			{
			root_0 = (CommonTree)adaptor.nil();


			// DotTree.g:281:9: ( STRICT )?
			int alt2=2;
			int LA2_0 = input.LA(1);
			if ( (LA2_0==STRICT) ) {
				alt2=1;
			}
			switch (alt2) {
				case 1 :
					// DotTree.g:281:10: STRICT
					{
					_last = (CommonTree)input.LT(1);
					STRICT5=(CommonTree)match(input,STRICT,FOLLOW_STRICT_in_graphModifier236); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					STRICT5_tree = (CommonTree)adaptor.dupNode(STRICT5);


					adaptor.addChild(root_0, STRICT5_tree);
					}

					if ( state.backtracking==0 ) {retval.isStrict =true;}
					if ( state.backtracking==0 ) {
					}

					}
					break;

			}

			// DotTree.g:281:37: ( GRAPH | DIGRAPH )
			int alt3=2;
			int LA3_0 = input.LA(1);
			if ( (LA3_0==GRAPH) ) {
				alt3=1;
			}
			else if ( (LA3_0==DIGRAPH) ) {
				alt3=2;
			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 3, 0, input);
				throw nvae;
			}

			switch (alt3) {
				case 1 :
					// DotTree.g:281:38: GRAPH
					{
					_last = (CommonTree)input.LT(1);
					GRAPH6=(CommonTree)match(input,GRAPH,FOLLOW_GRAPH_in_graphModifier243); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					GRAPH6_tree = (CommonTree)adaptor.dupNode(GRAPH6);


					adaptor.addChild(root_0, GRAPH6_tree);
					}

					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 2 :
					// DotTree.g:281:46: DIGRAPH
					{
					_last = (CommonTree)input.LT(1);
					DIGRAPH7=(CommonTree)match(input,DIGRAPH,FOLLOW_DIGRAPH_in_graphModifier247); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					DIGRAPH7_tree = (CommonTree)adaptor.dupNode(DIGRAPH7);


					adaptor.addChild(root_0, DIGRAPH7_tree);
					}

					if ( state.backtracking==0 ) {retval.isDirected =true;}
					if ( state.backtracking==0 ) {
					}

					}
					break;

			}

			if ( state.backtracking==0 ) {
			}

			}

			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "graphModifier"


	public static class stmt_list_return extends TreeRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "stmt_list"
	// DotTree.g:284:1: stmt_list : ^( STMT_LIST ( stmt )+ ) ;
	public final DotTree.stmt_list_return stmt_list() throws RecognitionException {
		DotTree.stmt_list_return retval = new DotTree.stmt_list_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree STMT_LIST8=null;
		TreeRuleReturnScope stmt9 =null;

		CommonTree STMT_LIST8_tree=null;

		try {
			// DotTree.g:285:5: ( ^( STMT_LIST ( stmt )+ ) )
			// DotTree.g:286:9: ^( STMT_LIST ( stmt )+ )
			{
			root_0 = (CommonTree)adaptor.nil();


			_last = (CommonTree)input.LT(1);
			{
			CommonTree _save_last_1 = _last;
			CommonTree _first_1 = null;
			CommonTree root_1 = (CommonTree)adaptor.nil();
			_last = (CommonTree)input.LT(1);
			STMT_LIST8=(CommonTree)match(input,STMT_LIST,FOLLOW_STMT_LIST_in_stmt_list276); if (state.failed) return retval;
			if ( state.backtracking==0 ) {
			STMT_LIST8_tree = (CommonTree)adaptor.dupNode(STMT_LIST8);


			root_1 = (CommonTree)adaptor.becomeRoot(STMT_LIST8_tree, root_1);
			}

			match(input, Token.DOWN, null); if (state.failed) return retval;
			// DotTree.g:286:21: ( stmt )+
			int cnt4=0;
			loop4:
			while (true) {
				int alt4=2;
				int LA4_0 = input.LA(1);
				if ( (LA4_0==ATTR||LA4_0==EDGE||LA4_0==EDGE_STMT||LA4_0==GRAPH||(LA4_0 >= NODE && LA4_0 <= NODE_STMT)||LA4_0==SUBGRAPH_ROOT) ) {
					alt4=1;
				}

				switch (alt4) {
				case 1 :
					// DotTree.g:286:21: stmt
					{
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_stmt_in_stmt_list278);
					stmt9=stmt();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, stmt9.getTree());

					if ( state.backtracking==0 ) {
					}

					}
					break;

				default :
					if ( cnt4 >= 1 ) break loop4;
					if (state.backtracking>0) {state.failed=true; return retval;}
					EarlyExitException eee = new EarlyExitException(4, input);
					throw eee;
				}
				cnt4++;
			}

			match(input, Token.UP, null); if (state.failed) return retval;
			adaptor.addChild(root_0, root_1);
			_last = _save_last_1;
			}


			if ( state.backtracking==0 ) {
			}

			}

			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "stmt_list"


	public static class stmt_return extends TreeRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "stmt"
	// DotTree.g:289:1: stmt : ( attr_stmt | subgraph | ^( ATTR n= ID EQUAL v= ID ) | edge_stmt | node_stmt );
	public final DotTree.stmt_return stmt() throws RecognitionException {
		DotTree.stmt_return retval = new DotTree.stmt_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree n=null;
		CommonTree v=null;
		CommonTree ATTR12=null;
		CommonTree EQUAL13=null;
		TreeRuleReturnScope attr_stmt10 =null;
		TreeRuleReturnScope subgraph11 =null;
		TreeRuleReturnScope edge_stmt14 =null;
		TreeRuleReturnScope node_stmt15 =null;

		CommonTree n_tree=null;
		CommonTree v_tree=null;
		CommonTree ATTR12_tree=null;
		CommonTree EQUAL13_tree=null;

		try {
			// DotTree.g:290:5: ( attr_stmt | subgraph | ^( ATTR n= ID EQUAL v= ID ) | edge_stmt | node_stmt )
			int alt5=5;
			switch ( input.LA(1) ) {
			case EDGE:
			case GRAPH:
			case NODE:
				{
				alt5=1;
				}
				break;
			case SUBGRAPH_ROOT:
				{
				alt5=2;
				}
				break;
			case ATTR:
				{
				alt5=3;
				}
				break;
			case EDGE_STMT:
				{
				alt5=4;
				}
				break;
			case NODE_STMT:
				{
				alt5=5;
				}
				break;
			default:
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 5, 0, input);
				throw nvae;
			}
			switch (alt5) {
				case 1 :
					// DotTree.g:291:9: attr_stmt
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_attr_stmt_in_stmt305);
					attr_stmt10=attr_stmt();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_0, attr_stmt10.getTree());

					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 2 :
					// DotTree.g:292:6: subgraph
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_subgraph_in_stmt314);
					subgraph11=subgraph();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_0, subgraph11.getTree());

					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 3 :
					// DotTree.g:293:6: ^( ATTR n= ID EQUAL v= ID )
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree)adaptor.nil();
					_last = (CommonTree)input.LT(1);
					ATTR12=(CommonTree)match(input,ATTR,FOLLOW_ATTR_in_stmt324); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					ATTR12_tree = (CommonTree)adaptor.dupNode(ATTR12);


					root_1 = (CommonTree)adaptor.becomeRoot(ATTR12_tree, root_1);
					}

					match(input, Token.DOWN, null); if (state.failed) return retval;
					_last = (CommonTree)input.LT(1);
					n=(CommonTree)match(input,ID,FOLLOW_ID_in_stmt328); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					n_tree = (CommonTree)adaptor.dupNode(n);


					adaptor.addChild(root_1, n_tree);
					}

					_last = (CommonTree)input.LT(1);
					EQUAL13=(CommonTree)match(input,EQUAL,FOLLOW_EQUAL_in_stmt330); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					EQUAL13_tree = (CommonTree)adaptor.dupNode(EQUAL13);


					adaptor.addChild(root_1, EQUAL13_tree);
					}

					_last = (CommonTree)input.LT(1);
					v=(CommonTree)match(input,ID,FOLLOW_ID_in_stmt334); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					v_tree = (CommonTree)adaptor.dupNode(v);


					adaptor.addChild(root_1, v_tree);
					}

					match(input, Token.UP, null); if (state.failed) return retval;
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
					}


					if ( state.backtracking==0 ) {
					            GraphOrSubgraph_stack.peek().obj.attributes.put((n!=null?n.getText():null).toLowerCase(), (v!=null?v.getText():null));
					        }
					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 4 :
					// DotTree.g:297:6: edge_stmt
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_edge_stmt_in_stmt353);
					edge_stmt14=edge_stmt();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_0, edge_stmt14.getTree());

					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 5 :
					// DotTree.g:298:6: node_stmt
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_node_stmt_in_stmt362);
					node_stmt15=node_stmt();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_0, node_stmt15.getTree());

					if ( state.backtracking==0 ) {
					}

					}
					break;

			}
			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "stmt"


	public static class attr_stmt_return extends TreeRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "attr_stmt"
	// DotTree.g:301:1: attr_stmt : ( ^( GRAPH a= attr_list ) | ^( NODE a= attr_list ) | ^( EDGE a= attr_list ) );
	public final DotTree.attr_stmt_return attr_stmt() throws RecognitionException {
		DotTree.attr_stmt_return retval = new DotTree.attr_stmt_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree GRAPH16=null;
		CommonTree NODE17=null;
		CommonTree EDGE18=null;
		TreeRuleReturnScope a =null;

		CommonTree GRAPH16_tree=null;
		CommonTree NODE17_tree=null;
		CommonTree EDGE18_tree=null;

		try {
			// DotTree.g:302:5: ( ^( GRAPH a= attr_list ) | ^( NODE a= attr_list ) | ^( EDGE a= attr_list ) )
			int alt6=3;
			switch ( input.LA(1) ) {
			case GRAPH:
				{
				alt6=1;
				}
				break;
			case NODE:
				{
				alt6=2;
				}
				break;
			case EDGE:
				{
				alt6=3;
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
					// DotTree.g:303:9: ^( GRAPH a= attr_list )
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree)adaptor.nil();
					_last = (CommonTree)input.LT(1);
					GRAPH16=(CommonTree)match(input,GRAPH,FOLLOW_GRAPH_in_attr_stmt389); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					GRAPH16_tree = (CommonTree)adaptor.dupNode(GRAPH16);


					root_1 = (CommonTree)adaptor.becomeRoot(GRAPH16_tree, root_1);
					}

					match(input, Token.DOWN, null); if (state.failed) return retval;
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_attr_list_in_attr_stmt393);
					a=attr_list();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, a.getTree());

					if ( state.backtracking==0 ) { GraphOrSubgraph_stack.peek().obj.graphAttributes.putAll((a!=null?((DotTree.attr_list_return)a).attributes:null));}
					match(input, Token.UP, null); if (state.failed) return retval;
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
					}


					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 2 :
					// DotTree.g:304:9: ^( NODE a= attr_list )
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree)adaptor.nil();
					_last = (CommonTree)input.LT(1);
					NODE17=(CommonTree)match(input,NODE,FOLLOW_NODE_in_attr_stmt410); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					NODE17_tree = (CommonTree)adaptor.dupNode(NODE17);


					root_1 = (CommonTree)adaptor.becomeRoot(NODE17_tree, root_1);
					}

					match(input, Token.DOWN, null); if (state.failed) return retval;
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_attr_list_in_attr_stmt414);
					a=attr_list();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, a.getTree());

					if ( state.backtracking==0 ) { GraphOrSubgraph_stack.peek().obj.nodeAttributes.putAll((a!=null?((DotTree.attr_list_return)a).attributes:null));}
					match(input, Token.UP, null); if (state.failed) return retval;
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
					}


					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 3 :
					// DotTree.g:305:9: ^( EDGE a= attr_list )
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree)adaptor.nil();
					_last = (CommonTree)input.LT(1);
					EDGE18=(CommonTree)match(input,EDGE,FOLLOW_EDGE_in_attr_stmt431); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					EDGE18_tree = (CommonTree)adaptor.dupNode(EDGE18);


					root_1 = (CommonTree)adaptor.becomeRoot(EDGE18_tree, root_1);
					}

					match(input, Token.DOWN, null); if (state.failed) return retval;
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_attr_list_in_attr_stmt435);
					a=attr_list();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, a.getTree());

					if ( state.backtracking==0 ) { GraphOrSubgraph_stack.peek().obj.edgeAttributes.putAll((a!=null?((DotTree.attr_list_return)a).attributes:null));}
					match(input, Token.UP, null); if (state.failed) return retval;
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
					}


					if ( state.backtracking==0 ) {
					}

					}
					break;

			}
			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "attr_stmt"


	public static class attr_list_return extends TreeRuleReturnScope {
		public Map attributes;
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "attr_list"
	// DotTree.g:308:1: attr_list returns [Map attributes] : ^( ATTR_LIST ( attr )* ) ;
	public final DotTree.attr_list_return attr_list() throws RecognitionException {
		DotTree.attr_list_return retval = new DotTree.attr_list_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree ATTR_LIST19=null;
		TreeRuleReturnScope attr20 =null;

		CommonTree ATTR_LIST19_tree=null;


		        retval.attributes = new HashMap<String, String>();
		    
		try {
			// DotTree.g:312:5: ( ^( ATTR_LIST ( attr )* ) )
			// DotTree.g:313:6: ^( ATTR_LIST ( attr )* )
			{
			root_0 = (CommonTree)adaptor.nil();


			_last = (CommonTree)input.LT(1);
			{
			CommonTree _save_last_1 = _last;
			CommonTree _first_1 = null;
			CommonTree root_1 = (CommonTree)adaptor.nil();
			_last = (CommonTree)input.LT(1);
			ATTR_LIST19=(CommonTree)match(input,ATTR_LIST,FOLLOW_ATTR_LIST_in_attr_list474); if (state.failed) return retval;
			if ( state.backtracking==0 ) {
			ATTR_LIST19_tree = (CommonTree)adaptor.dupNode(ATTR_LIST19);


			root_1 = (CommonTree)adaptor.becomeRoot(ATTR_LIST19_tree, root_1);
			}

			if ( input.LA(1)==Token.DOWN ) {
				match(input, Token.DOWN, null); if (state.failed) return retval;
				// DotTree.g:313:18: ( attr )*
				loop7:
				while (true) {
					int alt7=2;
					int LA7_0 = input.LA(1);
					if ( (LA7_0==ATTR) ) {
						alt7=1;
					}

					switch (alt7) {
					case 1 :
						// DotTree.g:313:19: attr
						{
						_last = (CommonTree)input.LT(1);
						pushFollow(FOLLOW_attr_in_attr_list477);
						attr20=attr();
						state._fsp--;
						if (state.failed) return retval;
						if ( state.backtracking==0 ) 
						adaptor.addChild(root_1, attr20.getTree());

						if ( state.backtracking==0 ) {retval.attributes.put((attr20!=null?((DotTree.attr_return)attr20).name:null).toLowerCase(), (attr20!=null?((DotTree.attr_return)attr20).value:null));}
						if ( state.backtracking==0 ) {
						}

						}
						break;

					default :
						break loop7;
					}
				}

				match(input, Token.UP, null); if (state.failed) return retval;
			}
			adaptor.addChild(root_0, root_1);
			_last = _save_last_1;
			}


			if ( state.backtracking==0 ) {
			}

			}

			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "attr_list"


	public static class attr_return extends TreeRuleReturnScope {
		public String name;
		public String value;
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "attr"
	// DotTree.g:316:1: attr returns [String name, String value] : ^( ATTR n= ID ( EQUAL v= ID )? ) ;
	public final DotTree.attr_return attr() throws RecognitionException {
		DotTree.attr_return retval = new DotTree.attr_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree n=null;
		CommonTree v=null;
		CommonTree ATTR21=null;
		CommonTree EQUAL22=null;

		CommonTree n_tree=null;
		CommonTree v_tree=null;
		CommonTree ATTR21_tree=null;
		CommonTree EQUAL22_tree=null;

		try {
			// DotTree.g:317:5: ( ^( ATTR n= ID ( EQUAL v= ID )? ) )
			// DotTree.g:318:9: ^( ATTR n= ID ( EQUAL v= ID )? )
			{
			root_0 = (CommonTree)adaptor.nil();


			_last = (CommonTree)input.LT(1);
			{
			CommonTree _save_last_1 = _last;
			CommonTree _first_1 = null;
			CommonTree root_1 = (CommonTree)adaptor.nil();
			_last = (CommonTree)input.LT(1);
			ATTR21=(CommonTree)match(input,ATTR,FOLLOW_ATTR_in_attr512); if (state.failed) return retval;
			if ( state.backtracking==0 ) {
			ATTR21_tree = (CommonTree)adaptor.dupNode(ATTR21);


			root_1 = (CommonTree)adaptor.becomeRoot(ATTR21_tree, root_1);
			}

			match(input, Token.DOWN, null); if (state.failed) return retval;
			_last = (CommonTree)input.LT(1);
			n=(CommonTree)match(input,ID,FOLLOW_ID_in_attr516); if (state.failed) return retval;
			if ( state.backtracking==0 ) {
			n_tree = (CommonTree)adaptor.dupNode(n);


			adaptor.addChild(root_1, n_tree);
			}

			// DotTree.g:318:21: ( EQUAL v= ID )?
			int alt8=2;
			int LA8_0 = input.LA(1);
			if ( (LA8_0==EQUAL) ) {
				alt8=1;
			}
			switch (alt8) {
				case 1 :
					// DotTree.g:318:22: EQUAL v= ID
					{
					_last = (CommonTree)input.LT(1);
					EQUAL22=(CommonTree)match(input,EQUAL,FOLLOW_EQUAL_in_attr519); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					EQUAL22_tree = (CommonTree)adaptor.dupNode(EQUAL22);


					adaptor.addChild(root_1, EQUAL22_tree);
					}

					_last = (CommonTree)input.LT(1);
					v=(CommonTree)match(input,ID,FOLLOW_ID_in_attr523); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					v_tree = (CommonTree)adaptor.dupNode(v);


					adaptor.addChild(root_1, v_tree);
					}

					if ( state.backtracking==0 ) {
					}

					}
					break;

			}

			match(input, Token.UP, null); if (state.failed) return retval;
			adaptor.addChild(root_0, root_1);
			_last = _save_last_1;
			}


			if ( state.backtracking==0 ) {retval.name = (n!=null?n.getText():null); retval.value =(v!=null?v.getText():null);}
			if ( state.backtracking==0 ) {
			}

			}

			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "attr"


	protected static class edge_stmt_scope {
		Edge edgeObj;
	}
	protected Stack<edge_stmt_scope> edge_stmt_stack = new Stack<edge_stmt_scope>();

	public static class edge_stmt_return extends TreeRuleReturnScope {
		public Edge edge;
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "edge_stmt"
	// DotTree.g:321:1: edge_stmt returns [Edge edge] : ( ^( EDGE_STMT n= node_id edgeRHS ( attr_list )? ) | ^( EDGE_STMT s= subgraph edgeRHS ( attr_list )? ) );
	public final DotTree.edge_stmt_return edge_stmt() throws RecognitionException {
		edge_stmt_stack.push(new edge_stmt_scope());
		DotTree.edge_stmt_return retval = new DotTree.edge_stmt_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree EDGE_STMT23=null;
		CommonTree EDGE_STMT26=null;
		TreeRuleReturnScope n =null;
		TreeRuleReturnScope s =null;
		TreeRuleReturnScope edgeRHS24 =null;
		TreeRuleReturnScope attr_list25 =null;
		TreeRuleReturnScope edgeRHS27 =null;
		TreeRuleReturnScope attr_list28 =null;

		CommonTree EDGE_STMT23_tree=null;
		CommonTree EDGE_STMT26_tree=null;


		        retval.edge = new Edge();
		        edge_stmt_stack.peek().edgeObj = retval.edge;
		        graph_stack.peek().obj.edges.add(retval.edge);
		    
		try {
			// DotTree.g:330:5: ( ^( EDGE_STMT n= node_id edgeRHS ( attr_list )? ) | ^( EDGE_STMT s= subgraph edgeRHS ( attr_list )? ) )
			int alt11=2;
			int LA11_0 = input.LA(1);
			if ( (LA11_0==EDGE_STMT) ) {
				int LA11_1 = input.LA(2);
				if ( (LA11_1==DOWN) ) {
					int LA11_2 = input.LA(3);
					if ( (LA11_2==ID) ) {
						alt11=1;
					}
					else if ( (LA11_2==SUBGRAPH_ROOT) ) {
						alt11=2;
					}

					else {
						if (state.backtracking>0) {state.failed=true; return retval;}
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 11, 2, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 11, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 11, 0, input);
				throw nvae;
			}

			switch (alt11) {
				case 1 :
					// DotTree.g:331:8: ^( EDGE_STMT n= node_id edgeRHS ( attr_list )? )
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree)adaptor.nil();
					_last = (CommonTree)input.LT(1);
					EDGE_STMT23=(CommonTree)match(input,EDGE_STMT,FOLLOW_EDGE_STMT_in_edge_stmt574); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					EDGE_STMT23_tree = (CommonTree)adaptor.dupNode(EDGE_STMT23);


					root_1 = (CommonTree)adaptor.becomeRoot(EDGE_STMT23_tree, root_1);
					}

					match(input, Token.DOWN, null); if (state.failed) return retval;
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_node_id_in_edge_stmt590);
					n=node_id();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, n.getTree());

					if ( state.backtracking==0 ) {
					                Node node = null;
					                if (graph_stack.peek().obj.nodes.containsKey((n!=null?((DotTree.node_id_return)n).id:null))) {
					                    node = graph_stack.peek().obj.nodes.get((n!=null?((DotTree.node_id_return)n).id:null));
					                } else {
					                    node=new Node((n!=null?((DotTree.node_id_return)n).id:null));
					                    graph_stack.peek().obj.nodes.put(node.id, node);
					                }
					                GraphOrSubgraph_stack.peek().obj.nodes.put(node.id, node);
					                retval.edge.nodes.add(node);
					                // NOTE: the port information should be somehow saved in edge too
					                // however, it is ignored right now
					           }
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_edgeRHS_in_edge_stmt616);
					edgeRHS24=edgeRHS();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, edgeRHS24.getTree());

					// DotTree.g:347:12: ( attr_list )?
					int alt9=2;
					int LA9_0 = input.LA(1);
					if ( (LA9_0==ATTR_LIST) ) {
						alt9=1;
					}
					switch (alt9) {
						case 1 :
							// DotTree.g:347:12: attr_list
							{
							_last = (CommonTree)input.LT(1);
							pushFollow(FOLLOW_attr_list_in_edge_stmt629);
							attr_list25=attr_list();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) 
							adaptor.addChild(root_1, attr_list25.getTree());

							if ( state.backtracking==0 ) {
							}

							}
							break;

					}

					if ( state.backtracking==0 ) {
					                retval.edge.attributes.putAll((attr_list25!=null?((DotTree.attr_list_return)attr_list25).attributes:null));
					           }
					match(input, Token.UP, null); if (state.failed) return retval;
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
					}


					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 2 :
					// DotTree.g:353:9: ^( EDGE_STMT s= subgraph edgeRHS ( attr_list )? )
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree)adaptor.nil();
					_last = (CommonTree)input.LT(1);
					EDGE_STMT26=(CommonTree)match(input,EDGE_STMT,FOLLOW_EDGE_STMT_in_edge_stmt669); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					EDGE_STMT26_tree = (CommonTree)adaptor.dupNode(EDGE_STMT26);


					root_1 = (CommonTree)adaptor.becomeRoot(EDGE_STMT26_tree, root_1);
					}

					match(input, Token.DOWN, null); if (state.failed) return retval;
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_subgraph_in_edge_stmt686);
					s=subgraph();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, s.getTree());

					if ( state.backtracking==0 ) {
					                retval.edge.nodes.add((s!=null?((DotTree.subgraph_return)s).subGraphObj:null));
					            }
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_edgeRHS_in_edge_stmt714);
					edgeRHS27=edgeRHS();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, edgeRHS27.getTree());

					// DotTree.g:359:13: ( attr_list )?
					int alt10=2;
					int LA10_0 = input.LA(1);
					if ( (LA10_0==ATTR_LIST) ) {
						alt10=1;
					}
					switch (alt10) {
						case 1 :
							// DotTree.g:359:13: attr_list
							{
							_last = (CommonTree)input.LT(1);
							pushFollow(FOLLOW_attr_list_in_edge_stmt728);
							attr_list28=attr_list();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) 
							adaptor.addChild(root_1, attr_list28.getTree());

							if ( state.backtracking==0 ) {
							}

							}
							break;

					}

					if ( state.backtracking==0 ) {
					                retval.edge.attributes.putAll((attr_list28!=null?((DotTree.attr_list_return)attr_list28).attributes:null));
					            }
					match(input, Token.UP, null); if (state.failed) return retval;
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
					}


					if ( state.backtracking==0 ) {
					}

					}
					break;

			}
			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
			edge_stmt_stack.pop();
		}
		return retval;
	}
	// $ANTLR end "edge_stmt"


	public static class edgeRHS_return extends TreeRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "edgeRHS"
	// DotTree.g:367:1: edgeRHS : ( ^( EDGEOP n= node_id ( edgeRHS )? ) | ^( EDGEOP subgraph ( edgeRHS )? ) );
	public final DotTree.edgeRHS_return edgeRHS() throws RecognitionException {
		DotTree.edgeRHS_return retval = new DotTree.edgeRHS_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree EDGEOP29=null;
		CommonTree EDGEOP31=null;
		TreeRuleReturnScope n =null;
		TreeRuleReturnScope edgeRHS30 =null;
		TreeRuleReturnScope subgraph32 =null;
		TreeRuleReturnScope edgeRHS33 =null;

		CommonTree EDGEOP29_tree=null;
		CommonTree EDGEOP31_tree=null;

		try {
			// DotTree.g:368:5: ( ^( EDGEOP n= node_id ( edgeRHS )? ) | ^( EDGEOP subgraph ( edgeRHS )? ) )
			int alt14=2;
			int LA14_0 = input.LA(1);
			if ( (LA14_0==EDGEOP) ) {
				int LA14_1 = input.LA(2);
				if ( (LA14_1==DOWN) ) {
					int LA14_2 = input.LA(3);
					if ( (LA14_2==ID) ) {
						alt14=1;
					}
					else if ( (LA14_2==SUBGRAPH_ROOT) ) {
						alt14=2;
					}

					else {
						if (state.backtracking>0) {state.failed=true; return retval;}
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 14, 2, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}

				else {
					if (state.backtracking>0) {state.failed=true; return retval;}
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 14, 0, input);
				throw nvae;
			}

			switch (alt14) {
				case 1 :
					// DotTree.g:369:9: ^( EDGEOP n= node_id ( edgeRHS )? )
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree)adaptor.nil();
					_last = (CommonTree)input.LT(1);
					EDGEOP29=(CommonTree)match(input,EDGEOP,FOLLOW_EDGEOP_in_edgeRHS788); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					EDGEOP29_tree = (CommonTree)adaptor.dupNode(EDGEOP29);


					root_1 = (CommonTree)adaptor.becomeRoot(EDGEOP29_tree, root_1);
					}

					match(input, Token.DOWN, null); if (state.failed) return retval;
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_node_id_in_edgeRHS792);
					n=node_id();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, n.getTree());

					if ( state.backtracking==0 ) {
					                Node node = null;
					                if (graph_stack.peek().obj.nodes.containsKey((n!=null?((DotTree.node_id_return)n).id:null))) {
					                    node = graph_stack.peek().obj.nodes.get((n!=null?((DotTree.node_id_return)n).id:null));
					                } else {
					                    node=new Node((n!=null?((DotTree.node_id_return)n).id:null));
					                    graph_stack.peek().obj.nodes.put(node.id, node);
					                }
					                GraphOrSubgraph_stack.peek().obj.nodes.put(node.id, node);                
					                edge_stmt_stack.peek().edgeObj.nodes.add(node);
					            }
					// DotTree.g:380:15: ( edgeRHS )?
					int alt12=2;
					int LA12_0 = input.LA(1);
					if ( (LA12_0==EDGEOP) ) {
						alt12=1;
					}
					switch (alt12) {
						case 1 :
							// DotTree.g:380:16: edgeRHS
							{
							_last = (CommonTree)input.LT(1);
							pushFollow(FOLLOW_edgeRHS_in_edgeRHS809);
							edgeRHS30=edgeRHS();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) 
							adaptor.addChild(root_1, edgeRHS30.getTree());

							if ( state.backtracking==0 ) {
							}

							}
							break;

					}

					match(input, Token.UP, null); if (state.failed) return retval;
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
					}


					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 2 :
					// DotTree.g:381:9: ^( EDGEOP subgraph ( edgeRHS )? )
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree)adaptor.nil();
					_last = (CommonTree)input.LT(1);
					EDGEOP31=(CommonTree)match(input,EDGEOP,FOLLOW_EDGEOP_in_edgeRHS825); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					EDGEOP31_tree = (CommonTree)adaptor.dupNode(EDGEOP31);


					root_1 = (CommonTree)adaptor.becomeRoot(EDGEOP31_tree, root_1);
					}

					match(input, Token.DOWN, null); if (state.failed) return retval;
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_subgraph_in_edgeRHS827);
					subgraph32=subgraph();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, subgraph32.getTree());

					if ( state.backtracking==0 ) {
					                edge_stmt_stack.peek().edgeObj.nodes.add((subgraph32!=null?((DotTree.subgraph_return)subgraph32).subGraphObj:null));
					            }
					// DotTree.g:385:13: ( edgeRHS )?
					int alt13=2;
					int LA13_0 = input.LA(1);
					if ( (LA13_0==EDGEOP) ) {
						alt13=1;
					}
					switch (alt13) {
						case 1 :
							// DotTree.g:385:14: edgeRHS
							{
							_last = (CommonTree)input.LT(1);
							pushFollow(FOLLOW_edgeRHS_in_edgeRHS857);
							edgeRHS33=edgeRHS();
							state._fsp--;
							if (state.failed) return retval;
							if ( state.backtracking==0 ) 
							adaptor.addChild(root_1, edgeRHS33.getTree());

							if ( state.backtracking==0 ) {
							}

							}
							break;

					}

					match(input, Token.UP, null); if (state.failed) return retval;
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
					}


					if ( state.backtracking==0 ) {
					}

					}
					break;

			}
			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "edgeRHS"


	public static class node_stmt_return extends TreeRuleReturnScope {
		public Node node;
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "node_stmt"
	// DotTree.g:389:1: node_stmt returns [Node node] : ^( NODE_STMT n= node_id (a= attr_list )? ) ;
	public final DotTree.node_stmt_return node_stmt() throws RecognitionException {
		DotTree.node_stmt_return retval = new DotTree.node_stmt_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree NODE_STMT34=null;
		TreeRuleReturnScope n =null;
		TreeRuleReturnScope a =null;

		CommonTree NODE_STMT34_tree=null;

		try {
			// DotTree.g:390:5: ( ^( NODE_STMT n= node_id (a= attr_list )? ) )
			// DotTree.g:391:9: ^( NODE_STMT n= node_id (a= attr_list )? )
			{
			root_0 = (CommonTree)adaptor.nil();


			_last = (CommonTree)input.LT(1);
			{
			CommonTree _save_last_1 = _last;
			CommonTree _first_1 = null;
			CommonTree root_1 = (CommonTree)adaptor.nil();
			_last = (CommonTree)input.LT(1);
			NODE_STMT34=(CommonTree)match(input,NODE_STMT,FOLLOW_NODE_STMT_in_node_stmt899); if (state.failed) return retval;
			if ( state.backtracking==0 ) {
			NODE_STMT34_tree = (CommonTree)adaptor.dupNode(NODE_STMT34);


			root_1 = (CommonTree)adaptor.becomeRoot(NODE_STMT34_tree, root_1);
			}

			match(input, Token.DOWN, null); if (state.failed) return retval;
			_last = (CommonTree)input.LT(1);
			pushFollow(FOLLOW_node_id_in_node_stmt903);
			n=node_id();
			state._fsp--;
			if (state.failed) return retval;
			if ( state.backtracking==0 ) 
			adaptor.addChild(root_1, n.getTree());

			// DotTree.g:391:32: (a= attr_list )?
			int alt15=2;
			int LA15_0 = input.LA(1);
			if ( (LA15_0==ATTR_LIST) ) {
				alt15=1;
			}
			switch (alt15) {
				case 1 :
					// DotTree.g:391:32: a= attr_list
					{
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_attr_list_in_node_stmt907);
					a=attr_list();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, a.getTree());

					if ( state.backtracking==0 ) {
					}

					}
					break;

			}

			if ( state.backtracking==0 ) {
			            if (graph_stack.peek().obj.nodes.containsKey((n!=null?((DotTree.node_id_return)n).id:null))) {
			                retval.node = graph_stack.peek().obj.nodes.get((n!=null?((DotTree.node_id_return)n).id:null));
			                retval.node.attributes.putAll((a!=null?((DotTree.attr_list_return)a).attributes:null));
			            } else {
			                retval.node =new Node((n!=null?((DotTree.node_id_return)n).id:null), (a!=null?((DotTree.attr_list_return)a).attributes:null));
			                graph_stack.peek().obj.nodes.put(retval.node.id, retval.node);
			            }
			            // set reference also in subgraph
			            GraphOrSubgraph_stack.peek().obj.nodes.put(retval.node.id, retval.node);
			        }
			match(input, Token.UP, null); if (state.failed) return retval;
			adaptor.addChild(root_0, root_1);
			_last = _save_last_1;
			}


			if ( state.backtracking==0 ) {
			}

			}

			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "node_stmt"


	public static class node_id_return extends TreeRuleReturnScope {
		public String id;
		public String port;
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "node_id"
	// DotTree.g:404:1: node_id returns [String id, String port] : ^(ident= ID (p= port )? ) ;
	public final DotTree.node_id_return node_id() throws RecognitionException {
		DotTree.node_id_return retval = new DotTree.node_id_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree ident=null;
		TreeRuleReturnScope p =null;

		CommonTree ident_tree=null;

		try {
			// DotTree.g:405:5: ( ^(ident= ID (p= port )? ) )
			// DotTree.g:406:9: ^(ident= ID (p= port )? )
			{
			root_0 = (CommonTree)adaptor.nil();


			_last = (CommonTree)input.LT(1);
			{
			CommonTree _save_last_1 = _last;
			CommonTree _first_1 = null;
			CommonTree root_1 = (CommonTree)adaptor.nil();
			_last = (CommonTree)input.LT(1);
			ident=(CommonTree)match(input,ID,FOLLOW_ID_in_node_id943); if (state.failed) return retval;
			if ( state.backtracking==0 ) {
			ident_tree = (CommonTree)adaptor.dupNode(ident);


			root_1 = (CommonTree)adaptor.becomeRoot(ident_tree, root_1);
			}

			if ( input.LA(1)==Token.DOWN ) {
				match(input, Token.DOWN, null); if (state.failed) return retval;
				// DotTree.g:406:21: (p= port )?
				int alt16=2;
				int LA16_0 = input.LA(1);
				if ( (LA16_0==ID||LA16_0==VALIDSTR) ) {
					alt16=1;
				}
				switch (alt16) {
					case 1 :
						// DotTree.g:406:21: p= port
						{
						_last = (CommonTree)input.LT(1);
						pushFollow(FOLLOW_port_in_node_id947);
						p=port();
						state._fsp--;
						if (state.failed) return retval;
						if ( state.backtracking==0 ) 
						adaptor.addChild(root_1, p.getTree());

						if ( state.backtracking==0 ) {
						}

						}
						break;

				}

				match(input, Token.UP, null); if (state.failed) return retval;
			}
			adaptor.addChild(root_0, root_1);
			_last = _save_last_1;
			}


			if ( state.backtracking==0 ) {retval.id =(ident!=null?ident.getText():null); retval.port =(p!=null?(input.getTokenStream().toString(input.getTreeAdaptor().getTokenStartIndex(p.start),input.getTreeAdaptor().getTokenStopIndex(p.start))):null);}
			if ( state.backtracking==0 ) {
			}

			}

			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "node_id"


	public static class port_return extends TreeRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "port"
	// DotTree.g:410:1: port : ( ID ( VALIDSTR )? | VALIDSTR );
	public final DotTree.port_return port() throws RecognitionException {
		DotTree.port_return retval = new DotTree.port_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree ID35=null;
		CommonTree VALIDSTR36=null;
		CommonTree VALIDSTR37=null;

		CommonTree ID35_tree=null;
		CommonTree VALIDSTR36_tree=null;
		CommonTree VALIDSTR37_tree=null;

		try {
			// DotTree.g:411:5: ( ID ( VALIDSTR )? | VALIDSTR )
			int alt18=2;
			int LA18_0 = input.LA(1);
			if ( (LA18_0==ID) ) {
				alt18=1;
			}
			else if ( (LA18_0==VALIDSTR) ) {
				alt18=2;
			}

			else {
				if (state.backtracking>0) {state.failed=true; return retval;}
				NoViableAltException nvae =
					new NoViableAltException("", 18, 0, input);
				throw nvae;
			}

			switch (alt18) {
				case 1 :
					// DotTree.g:412:6: ID ( VALIDSTR )?
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					ID35=(CommonTree)match(input,ID,FOLLOW_ID_in_port985); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					ID35_tree = (CommonTree)adaptor.dupNode(ID35);


					adaptor.addChild(root_0, ID35_tree);
					}

					// DotTree.g:412:9: ( VALIDSTR )?
					int alt17=2;
					int LA17_0 = input.LA(1);
					if ( (LA17_0==VALIDSTR) ) {
						alt17=1;
					}
					switch (alt17) {
						case 1 :
							// DotTree.g:412:10: VALIDSTR
							{
							_last = (CommonTree)input.LT(1);
							VALIDSTR36=(CommonTree)match(input,VALIDSTR,FOLLOW_VALIDSTR_in_port988); if (state.failed) return retval;
							if ( state.backtracking==0 ) {
							VALIDSTR36_tree = (CommonTree)adaptor.dupNode(VALIDSTR36);


							adaptor.addChild(root_0, VALIDSTR36_tree);
							}

							if ( state.backtracking==0 ) {
							}

							}
							break;

					}

					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 2 :
					// DotTree.g:413:6: VALIDSTR
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					VALIDSTR37=(CommonTree)match(input,VALIDSTR,FOLLOW_VALIDSTR_in_port999); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					VALIDSTR37_tree = (CommonTree)adaptor.dupNode(VALIDSTR37);


					adaptor.addChild(root_0, VALIDSTR37_tree);
					}

					if ( state.backtracking==0 ) {
					}

					}
					break;

			}
			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "port"


	public static class subgraph_return extends TreeRuleReturnScope {
		public SubGraph subGraphObj;
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "subgraph"
	// DotTree.g:416:1: subgraph returns [SubGraph subGraphObj] : ( ^( SUBGRAPH_ROOT ( ID )? stmt_list ) | ^( SUBGRAPH_ROOT ID ) );
	public final DotTree.subgraph_return subgraph() throws RecognitionException {
		GraphOrSubgraph_stack.push(new GraphOrSubgraph_scope());

		DotTree.subgraph_return retval = new DotTree.subgraph_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		CommonTree _first_0 = null;
		CommonTree _last = null;


		CommonTree SUBGRAPH_ROOT38=null;
		CommonTree ID39=null;
		CommonTree SUBGRAPH_ROOT41=null;
		CommonTree ID42=null;
		TreeRuleReturnScope stmt_list40 =null;

		CommonTree SUBGRAPH_ROOT38_tree=null;
		CommonTree ID39_tree=null;
		CommonTree SUBGRAPH_ROOT41_tree=null;
		CommonTree ID42_tree=null;

		try {
			// DotTree.g:418:5: ( ^( SUBGRAPH_ROOT ( ID )? stmt_list ) | ^( SUBGRAPH_ROOT ID ) )
			int alt20=2;
			int LA20_0 = input.LA(1);
			if ( (LA20_0==SUBGRAPH_ROOT) ) {
				int LA20_1 = input.LA(2);
				if ( (LA20_1==DOWN) ) {
					int LA20_2 = input.LA(3);
					if ( (LA20_2==ID) ) {
						int LA20_3 = input.LA(4);
						if ( (LA20_3==UP) ) {
							alt20=2;
						}
						else if ( (LA20_3==STMT_LIST) ) {
							alt20=1;
						}

						else {
							if (state.backtracking>0) {state.failed=true; return retval;}
							int nvaeMark = input.mark();
							try {
								for (int nvaeConsume = 0; nvaeConsume < 4 - 1; nvaeConsume++) {
									input.consume();
								}
								NoViableAltException nvae =
									new NoViableAltException("", 20, 3, input);
								throw nvae;
							} finally {
								input.rewind(nvaeMark);
							}
						}

					}
					else if ( (LA20_2==STMT_LIST) ) {
						alt20=1;
					}

					else {
						if (state.backtracking>0) {state.failed=true; return retval;}
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 20, 2, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

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
					// DotTree.g:419:6: ^( SUBGRAPH_ROOT ( ID )? stmt_list )
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree)adaptor.nil();
					_last = (CommonTree)input.LT(1);
					SUBGRAPH_ROOT38=(CommonTree)match(input,SUBGRAPH_ROOT,FOLLOW_SUBGRAPH_ROOT_in_subgraph1035); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					SUBGRAPH_ROOT38_tree = (CommonTree)adaptor.dupNode(SUBGRAPH_ROOT38);


					root_1 = (CommonTree)adaptor.becomeRoot(SUBGRAPH_ROOT38_tree, root_1);
					}

					match(input, Token.DOWN, null); if (state.failed) return retval;
					// DotTree.g:420:13: ( ID )?
					int alt19=2;
					int LA19_0 = input.LA(1);
					if ( (LA19_0==ID) ) {
						alt19=1;
					}
					switch (alt19) {
						case 1 :
							// DotTree.g:420:13: ID
							{
							_last = (CommonTree)input.LT(1);
							ID39=(CommonTree)match(input,ID,FOLLOW_ID_in_subgraph1049); if (state.failed) return retval;
							if ( state.backtracking==0 ) {
							ID39_tree = (CommonTree)adaptor.dupNode(ID39);


							adaptor.addChild(root_1, ID39_tree);
							}

							if ( state.backtracking==0 ) {
							}

							}
							break;

					}

					if ( state.backtracking==0 ) {
					                if (ID39!=null) {
					                    if (graph_stack.peek().obj.subGraphsMap.containsKey((ID39!=null?ID39.getText():null))) {
					                        retval.subGraphObj = graph_stack.peek().obj.subGraphsMap.get((ID39!=null?ID39.getText():null));
					                    } else {
					                        retval.subGraphObj = new SubGraph((ID39!=null?ID39.getText():null));
					                    }
					                    graph_stack.peek().obj.subGraphsMap.put(retval.subGraphObj.id, retval.subGraphObj);
					                } else {
					                    retval.subGraphObj = new SubGraph();
					                }
					                graph_stack.peek().obj.subGraphs.add(retval.subGraphObj);
					                GraphOrSubgraph_stack.peek().obj = (Graph)retval.subGraphObj;
					            }
					_last = (CommonTree)input.LT(1);
					pushFollow(FOLLOW_stmt_list_in_subgraph1078);
					stmt_list40=stmt_list();
					state._fsp--;
					if (state.failed) return retval;
					if ( state.backtracking==0 ) 
					adaptor.addChild(root_1, stmt_list40.getTree());

					match(input, Token.UP, null); if (state.failed) return retval;
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
					}


					if ( state.backtracking==0 ) {
					}

					}
					break;
				case 2 :
					// DotTree.g:438:11: ^( SUBGRAPH_ROOT ID )
					{
					root_0 = (CommonTree)adaptor.nil();


					_last = (CommonTree)input.LT(1);
					{
					CommonTree _save_last_1 = _last;
					CommonTree _first_1 = null;
					CommonTree root_1 = (CommonTree)adaptor.nil();
					_last = (CommonTree)input.LT(1);
					SUBGRAPH_ROOT41=(CommonTree)match(input,SUBGRAPH_ROOT,FOLLOW_SUBGRAPH_ROOT_in_subgraph1111); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					SUBGRAPH_ROOT41_tree = (CommonTree)adaptor.dupNode(SUBGRAPH_ROOT41);


					root_1 = (CommonTree)adaptor.becomeRoot(SUBGRAPH_ROOT41_tree, root_1);
					}

					match(input, Token.DOWN, null); if (state.failed) return retval;
					_last = (CommonTree)input.LT(1);
					ID42=(CommonTree)match(input,ID,FOLLOW_ID_in_subgraph1113); if (state.failed) return retval;
					if ( state.backtracking==0 ) {
					ID42_tree = (CommonTree)adaptor.dupNode(ID42);


					adaptor.addChild(root_1, ID42_tree);
					}

					if ( state.backtracking==0 ) {
					            if (graph_stack.peek().obj.subGraphsMap.containsKey((ID42!=null?ID42.getText():null)))
					                retval.subGraphObj = graph_stack.peek().obj.subGraphsMap.get((ID42!=null?ID42.getText():null));
					            else
					                retval.subGraphObj = new SubGraph((ID42!=null?ID42.getText():null));

					            graph_stack.peek().obj.subGraphsMap.put(retval.subGraphObj.id, retval.subGraphObj);
					            graph_stack.peek().obj.subGraphs.add(retval.subGraphObj);
					            // not usefull, since this subgraph doesn't define any statements..
					            // GraphOrSubgraph_stack.peek().obj = (Graph)retval.subGraphObj;
					          }
					match(input, Token.UP, null); if (state.failed) return retval;
					adaptor.addChild(root_0, root_1);
					_last = _save_last_1;
					}


					if ( state.backtracking==0 ) {
					}

					}
					break;

			}
			if ( state.backtracking==0 ) {

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
			GraphOrSubgraph_stack.pop();

		}
		return retval;
	}
	// $ANTLR end "subgraph"

	// Delegated rules



	public static final BitSet FOLLOW_GRAPH_ROOT_in_graph137 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_graphModifier_in_graph151 = new BitSet(new long[]{0x0010000040000000L});
	public static final BitSet FOLLOW_ID_in_graph179 = new BitSet(new long[]{0x0010000000000000L});
	public static final BitSet FOLLOW_stmt_list_in_graph196 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_STRICT_in_graphModifier236 = new BitSet(new long[]{0x0000000002010000L});
	public static final BitSet FOLLOW_GRAPH_in_graphModifier243 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_DIGRAPH_in_graphModifier247 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_STMT_LIST_in_stmt_list276 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_stmt_in_stmt_list278 = new BitSet(new long[]{0x0100018002140048L});
	public static final BitSet FOLLOW_attr_stmt_in_stmt305 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_subgraph_in_stmt314 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ATTR_in_stmt324 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_ID_in_stmt328 = new BitSet(new long[]{0x0000000000200000L});
	public static final BitSet FOLLOW_EQUAL_in_stmt330 = new BitSet(new long[]{0x0000000040000000L});
	public static final BitSet FOLLOW_ID_in_stmt334 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_edge_stmt_in_stmt353 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_node_stmt_in_stmt362 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_GRAPH_in_attr_stmt389 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_attr_list_in_attr_stmt393 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_NODE_in_attr_stmt410 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_attr_list_in_attr_stmt414 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_EDGE_in_attr_stmt431 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_attr_list_in_attr_stmt435 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_ATTR_LIST_in_attr_list474 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_attr_in_attr_list477 = new BitSet(new long[]{0x0000000000000048L});
	public static final BitSet FOLLOW_ATTR_in_attr512 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_ID_in_attr516 = new BitSet(new long[]{0x0000000000200008L});
	public static final BitSet FOLLOW_EQUAL_in_attr519 = new BitSet(new long[]{0x0000000040000000L});
	public static final BitSet FOLLOW_ID_in_attr523 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_EDGE_STMT_in_edge_stmt574 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_node_id_in_edge_stmt590 = new BitSet(new long[]{0x0000000000080000L});
	public static final BitSet FOLLOW_edgeRHS_in_edge_stmt616 = new BitSet(new long[]{0x0000000000000088L});
	public static final BitSet FOLLOW_attr_list_in_edge_stmt629 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_EDGE_STMT_in_edge_stmt669 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_subgraph_in_edge_stmt686 = new BitSet(new long[]{0x0000000000080000L});
	public static final BitSet FOLLOW_edgeRHS_in_edge_stmt714 = new BitSet(new long[]{0x0000000000000088L});
	public static final BitSet FOLLOW_attr_list_in_edge_stmt728 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_EDGEOP_in_edgeRHS788 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_node_id_in_edgeRHS792 = new BitSet(new long[]{0x0000000000080008L});
	public static final BitSet FOLLOW_edgeRHS_in_edgeRHS809 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_EDGEOP_in_edgeRHS825 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_subgraph_in_edgeRHS827 = new BitSet(new long[]{0x0000000000080008L});
	public static final BitSet FOLLOW_edgeRHS_in_edgeRHS857 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_NODE_STMT_in_node_stmt899 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_node_id_in_node_stmt903 = new BitSet(new long[]{0x0000000000000088L});
	public static final BitSet FOLLOW_attr_list_in_node_stmt907 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_ID_in_node_id943 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_port_in_node_id947 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_ID_in_port985 = new BitSet(new long[]{0x1000000000000002L});
	public static final BitSet FOLLOW_VALIDSTR_in_port988 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_VALIDSTR_in_port999 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_SUBGRAPH_ROOT_in_subgraph1035 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_ID_in_subgraph1049 = new BitSet(new long[]{0x0010000000000000L});
	public static final BitSet FOLLOW_stmt_list_in_subgraph1078 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_SUBGRAPH_ROOT_in_subgraph1111 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_ID_in_subgraph1113 = new BitSet(new long[]{0x0000000000000008L});
}
