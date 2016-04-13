package etherpad.proxy.test;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import net.gjerull.etherpad.client.EPLiteClient;

/**
 * Standalone Etherpad client to test lowest latency to a Etherpad server.
 * 
 * @author gaozy
 * 
 */
public class EtherpadClient {
	
	final static String apiKey = "477304aadcaafc1ad3665e700b99583a4c21480b3e04939c3c6cd903ba72cd28";
	final static String DEFAULT_API_VERSION = "1.2.1";
	final static int REQ_LENTGH = 100;
	
	final static String padName = "foo";
	
	private static SecureRandom random = new SecureRandom();
	protected static String nextString() {
	    return new BigInteger(REQ_LENTGH, random).toString(32);
	}
	
	private static EPLiteClient client; 
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		String host = args[0];
		int num_req = Integer.parseInt(args[1]);
		if(num_req <= 0){
			System.out.println("Please enter num of requests largeer than 0.");
		}
		
		String hostName = "http://"+host+":" + 9001;
		client = new EPLiteClient(hostName, apiKey, DEFAULT_API_VERSION);
		ArrayList<Long> latency = new ArrayList<Long>();
		
		List<String> pads = (List<String>) client.listAllPads().get("padIDs");
		if(!pads.contains(padName)){
			client.createPad(padName);
		}
		
		// Warm up with the first 10 requests
		for(int i=0; i<10; i++){
			long t = System.currentTimeMillis();
			client.setText(padName, nextString());
			long eclapsed = System.currentTimeMillis() - t;
			System.out.println(eclapsed);
		}
		
		for (int i=0; i<num_req; i++){
			long t = System.currentTimeMillis();
			client.setText(padName, nextString());
			long eclapsed = System.currentTimeMillis() - t;
			System.out.println(eclapsed);
			latency.add(eclapsed);
		}
		
		long total = 0;
		for (long lat:latency){
			total += lat;
		}
		System.out.println("The average latency is "+total/latency.size());
	}
}
