package net.floodlightcontroller.multicastcontroller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.U64;

import net.floodlightcontroller.routing.Link;

public class TestMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
//		Link l1 = new Link();
//		Link l2 = new Link();
//		Link l3 = new Link();
//		
//		l1.setSrc(DatapathId.of("1"));
//		l1.setSrcPort(OFPort.ofInt(1));
//		l1.setDst(DatapathId.of("2"));
//		l1.setDstPort(OFPort.ofInt(1));
//		l1.setLatency(U64.of(10));
//		
//		l2.setSrc(DatapathId.of("2"));
//		l2.setSrcPort(OFPort.ofInt(1));
//		l2.setDst(DatapathId.of("1"));
//		l2.setDstPort(OFPort.ofInt(1));
//		l2.setLatency(U64.of(10));
//		
//		l3.setSrc(DatapathId.of("1"));
//		l3.setSrcPort(OFPort.ofInt(1));
//		l3.setDst(DatapathId.of("2"));
//		l3.setDstPort(OFPort.ofInt(1));
//		l3.setLatency(U64.of(10));
//
//		HashMap<Link, Integer> linkMap = new HashMap<>();
//		linkMap.put(l1, 10);
//		linkMap.put(l2, 20);
//		
//		BidirectionalLink bl1 = new BidirectionalLink(l1);
//		BidirectionalLink bl2 = new BidirectionalLink(l2);
//		BidirectionalLink bl3 = new BidirectionalLink(l3);
//
//		
//		HashMap<BidirectionalLink, Integer> bLinkMap = new HashMap<>();
//		HashSet<BidirectionalLink> bLinkSet = new HashSet<>();
//		bLinkMap.put(bl1, 10);
//		bLinkMap.put(bl2, 20);
//		
//		bLinkSet.add(bl1);
//		bLinkSet.add(bl2);
//
//		TreeMap<BidirectionalLink, Integer> bLinkTreeMap = new TreeMap();
//		bLinkTreeMap.put(bl1, 10);
//		bLinkTreeMap.put(bl2, 20);
//		
//		System.out.println("------------");
//		System.out.println(bLinkTreeMap.toString());
//		System.out.println("------------");
//		
//		System.out.println(bLinkTreeMap.get(bl3).toString());
		
		
//		HashMap<String, ArrayList<Integer>> testMap = new HashMap<String, ArrayList<Integer>> ();
//		String  str = "s1";
//		ArrayList<Integer>  al = new ArrayList<>();
//		testMap.put(str, al);
//		
//		ArrayList<Integer> tempAL = testMap.get(str);
//		tempAL.add(5);
//		tempAL.add(6);
//		
//		
//		System.out.println(testMap);
		
//		ArrayList<MulticastGroup> dAr = new ArrayList<MulticastGroup>();
//		MulticastGroup ts = new MulticastGroup("10.0.0.1", new MulticastController() ); 
//		dAr.add(ts);
//		
//		System.out.println("------------");
//		System.out.println(dAr.toString());
//		System.out.println("------------");
//		ts.groupIP = "10.0.0.10";
//		
//		System.out.println("------------");
//		System.out.println(dAr.toString());
//		System.out.println("------------");
		
		
		int i1 = 5;
		int i2 = 3;
		double d = 0;
		long l = 5;
		
		d =  i1/(double)i2;
		System.out.println(d);
		
		long ll = Long.parseLong("10", 16);
		
		System.out.println(Long.toString(ll, 16));

	}
}
