package net.floodlightcontroller.multicastcontroller;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.apache.derby.tools.sysinfo;
import org.cesta.parsers.dot.DotLexer;
import org.cesta.parsers.dot.DotParser;
import org.cesta.parsers.dot.DotTree;
import org.cesta.parsers.dot.DotParser.graph_return;
import org.projectfloodlight.openflow.util.HexString;
import org.python.antlr.ast.Str;

import ch.qos.logback.classic.Logger;


/*
 * This class reads topology from file and provides information like link capacity,
 *  not available due to technical limitation of emulation or simulation environment.
 */
public class TopologyHelper {
	
	/* bandwidth value passed are assumed in Mbps*/
	/* coverted in kbps to store at fine granularity*/
	private static final String bandwidthAttributeName = "bandwidth";
	private static final String dpidAttributeName = "datapathid";
	private static final String typeAttributeName = "type";
	private static final String MGMTportSwitchPropertyID = "mgmtport";
	private static final String NBCSwitchPropertyID = "nbc";
	
	public TopologyHelper(String dotFileName) {
		getDotTreeGraphFromFile(dotFileName);
		buildMaps();
	}

	DotTree.Graph graphObjRet = null;
	
	/* concatenated datapath id of nodes with leading zeros stripped of is the key
	 * Check GetLinkBandwidth function for reference
	 */
	private HashMap<String, Long> linkCapacityMap = new HashMap<>();
	
	private HashMap<Long, String> mgmtPortMap = new HashMap<>();
	
	private HashMap<Long, Double> nbcMap = new HashMap<>();

	
	/*
	 * returns bandwidth of 'the'(only one link allowed) link between nodes 
	 * in kbps
	 */
	public Double getNodeBetweennessCentrality(long dpId) {
		return nbcMap.get(dpId);
	}
	
	/*
	 * returns bandwidth of 'the'(only one link allowed) link between nodes 
	 * in kbps
	 */
	public long getLinkCapacity(long srcDpId, long dstDpId) {
		long bandwidth;
		String key = Long.toString(srcDpId) + Long.toString(dstDpId);
//		System.out.println("getLinkCapacity : key " + key);
		bandwidth = linkCapacityMap.get(key);
		return bandwidth;
	}
	
	/*
	 * returns management port for the datapath 
	 */
	public String getManagementPort(long dpid) {
//		System.out.println(" dpid ="+Long.toString(dpid) + " mgmt port = " + mgmtPortMap.get(dpid));
		return mgmtPortMap.get(dpid);
	}
	
	private void getDotTreeGraphFromFile (String dotFileName) {		
		
		FileInputStream file;
		try {
			file = new FileInputStream(dotFileName);		
			ANTLRInputStream input = new ANTLRInputStream(file);
			DotLexer lexer = new DotLexer(input);
			CommonTokenStream tokens = new CommonTokenStream(lexer);
			
			DotParser parser = new DotParser(tokens);
			graph_return ret = parser.graph();
			CommonTree tree = ret.getTree();
			CommonTreeNodeStream ctnsNodes = new CommonTreeNodeStream(tree);
			DotTree dotTree = new DotTree(ctnsNodes);
			graphObjRet = dotTree.graph().graphObj;
			
			removeQuotesFromPropertyValueOfGrpah(graphObjRet);
//			System.out.println (graphObjRet.id);
			
			for (DotTree.Node n : graphObjRet.getNodes()) {
				//System.out.println (n.toString());
			}
			
			for (DotTree.NodePair np : graphObjRet.getNodePairs()) {
				//System.out.println (np.toString());
			}
			
		} catch (Exception e) {
			System.out.println("error in reading file named - "+dotFileName);
			//e.printStackTrace();
		}
	}
	
	private void buildMaps() {
		for (DotTree.NodePair np : graphObjRet.getNodePairs()) {
			String typeX = np.x.attributes.get(typeAttributeName);
			String typeY = np.y.attributes.get(typeAttributeName);
			if (!typeX.equals("switch") || !typeY.equals("switch"))
				continue;
			String dpidx = np.x.attributes.get(dpidAttributeName);
			String dpidy = np.y.attributes.get(dpidAttributeName);
			long dpidxHex = Long.parseLong(dpidx, 16);
			long dpidyHex = Long.parseLong(dpidy, 16);
			mgmtPortMap.put(dpidxHex, np.x.attributes.get(MGMTportSwitchPropertyID));
			mgmtPortMap.put(dpidyHex, np.y.attributes.get(MGMTportSwitchPropertyID));
			String dpx = Long.toString(dpidxHex);
			String dpy = Long.toString(dpidyHex);
			String key1 = dpx + dpy;
			String key2 = dpy + dpx;
			long bw = Integer.parseInt(np.attributes.get(bandwidthAttributeName)) * 1000;
			linkCapacityMap.put(key1, bw);
			linkCapacityMap.put(key2, bw);
		}
		
		for (DotTree.Node n : graphObjRet.getNodes()) {
			String nodeName = n.attributes.get(typeAttributeName);
			String dpidNode = n.attributes.get(dpidAttributeName);
			String type = n.attributes.get(typeAttributeName);
			if (!type.equals("switch"))
				continue;
			long dpidxHex = Long.parseLong(dpidNode, 16);
			
			mgmtPortMap.put(dpidxHex, n.attributes.get(MGMTportSwitchPropertyID));
			nbcMap.put(dpidxHex, Double.parseDouble(n.attributes.get( NBCSwitchPropertyID)));
		}
		
//		System.out.println(mgmtPortMap.toString());
//		System.out.println(linkCapacityMap.toString());
		
	}
	
	private void removeQuotesFromPropertyValueOfGrpah(DotTree.Graph graphObj)  {
		for (Map.Entry<String, DotTree.Node> en1 : graphObj.nodes.entrySet()) {
			for (Map.Entry<String, String> en2 : en1.getValue().attributes.entrySet()) {
				String str = en2.getValue();
				if (str.length() > 2 && str.startsWith("\"") && str.endsWith("\"")) {
					str = str.substring(1,str.length()-1);
					en2.setValue(str);
				}
			}
		}
	}

//	public HashMap<String, ArrayList<String>> getMappingsFromFile (String mappingFileName) {
//		HashMap<String, ArrayList<String>> mapping = new HashMap<String, ArrayList<String>> ();
//		FileInputStream fs;
//		try {
//			fs = new FileInputStream(mappingFileName);
//			BufferedReader br = new BufferedReader(new InputStreamReader(fs));
//			
//			String physNodeId = null;
//			String line = null;
//			String[] nodeIds = null;
//			String[] vtNodeIds = null;
//
//			while ((line = br.readLine()) != null) {
//				ArrayList<String> vtNodeIdsArrayList = new ArrayList<String>();
//				nodeIds = line.split("-");
//				
//				if (nodeIds.length != 2) {
//					System.out.println("error in file named - "+mappingFileName + " at line : --" +line);
//					System.out.println("error in reading file named - "+mappingFileName);
//					return null;
//				}
//				
//				physNodeId = nodeIds[0].trim();
//				vtNodeIds = nodeIds[1].trim().split(",");
//				for (String id : vtNodeIds) {
//					vtNodeIdsArrayList.add(id.trim());
//				}
//				mapping.put(physNodeId, vtNodeIdsArrayList);
//			}
//		 
//			br.close();
//		} catch (Exception e) {
//			System.out.println("Exception thrown: error in reading file named - "+mappingFileName);
//		}
//		return mapping;
//	}
}
