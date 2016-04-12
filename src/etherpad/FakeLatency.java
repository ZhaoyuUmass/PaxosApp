package etherpad;


import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;

import edu.umass.cs.gigapaxos.PaxosConfig;

/**
 * @author gaozy
 *
 */
public class FakeLatency {
	private static HashMap<String, HashMap<String, Double>> latencies = new HashMap<String, HashMap<String, Double>>();
	private final static double DEFAULT_HIGHEST_LATENCY = 500;	
	
	private static HashMap<InetAddress, String> addressToName = new HashMap<InetAddress, String>();
	
	protected FakeLatency() {
		for (String name:PaxosConfig.getActives().keySet()){
			latencies.put(name, new HashMap<String, Double>());
			addressToName.put(PaxosConfig.getActives().get(name).getAddress(), name);
		}
		
		
		StringBuilder builder = new StringBuilder();
		FileInputStream fis = null;
		try{
			fis = new FileInputStream("table");		
			int ch;
			while((ch = fis.read()) != -1){
			    builder.append((char)ch);
			}
			fis.close();
		} catch(IOException e){
			e.printStackTrace();
		}
		
		String lat = builder.toString();
		String[] lats = lat.split("\n");
				
		int i = 0;
		for (String client:PaxosConfig.getActives().keySet()){
			int j = 0;
			for (String name:PaxosConfig.getActives().keySet()){
				String[] l = lats[i].split(" ");
				latencies.get(client).put(name, Double.parseDouble(l[j]));
				j++;
			}
			i++;
		}
		
	}
	
	protected ArrayList<String> getClosest(String client){
		ArrayList<String> names = new ArrayList<String>();
		names.add(client);

		HashMap<String, Double> lats = latencies.get(client);
		System.out.println(this.getClass().getSimpleName()+" getClosest "+lats+" "+client);
		double lowest = DEFAULT_HIGHEST_LATENCY;
		String closest = null;
		for(String name:lats.keySet()){
			if(name.equals(client)){
				continue;
			}
			double lat = lats.get(name);
			if( lat < lowest){
				lowest = lat;
				closest = name;
			}
		}
		assert(closest != null);
		names.add(closest);
		
		return names;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		FakeLatency policy = new FakeLatency();
		System.out.println(policy.getClosest("100"));
		System.out.println(policy.getClosest("101"));
		System.out.println(policy.getClosest("102"));
		System.out.println(policy.getClosest("103"));
		System.out.println(policy.getClosest("104"));
	}
}
