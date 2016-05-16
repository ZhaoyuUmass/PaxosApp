package policy;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.nioutils.RTTEstimator;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.InterfaceGetActiveIPs;

/**
 * @author gaozy
 *
 */
public class ReconfigurableProfile extends AbstractDemandProfile{
	private final static int RECONFIGURATION_THRESHOLD = 20;
	private final static int REPORT_THRESHOLD = 4;
	private final static String SERVICE_NAME = "service_name";
	private final static String NUM_REQ = "num_request";
	private final static String HOST = "host";
	private final static int GROUP_SIZE = 2;
	
	private Integer numReq = 0;
	private ReconfigurableProfile lastReconfiguredProfile = null;
	
	//private InetAddress mostActiveRegion = null;
	private String mostActiveRegions = null;
	
	/**
	 * @param name
	 */
	public ReconfigurableProfile(String name) {
		super(name);
	}
	
	/**
	 * @param nnp
	 */
	public ReconfigurableProfile(ReconfigurableProfile nnp) {
		super(nnp.name);
		this.numReq = nnp.numReq;
		this.mostActiveRegions = nnp.mostActiveRegions;
	}

	@Override
	public ReconfigurableProfile clone() {
		return new ReconfigurableProfile(this);	
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public ReconfigurableProfile(JSONObject json) throws JSONException {
		super(json.getString(SERVICE_NAME));
		this.mostActiveRegions = json.getString(HOST);
		this.numReq = json.getInt(NUM_REQ);
	}
	
	
	@Override
	public void combine(AbstractDemandProfile dp) {
		ReconfigurableProfile update = (ReconfigurableProfile) dp;
		if( ! update.mostActiveRegions.equals(this.mostActiveRegions)){
			this.mostActiveRegions = update.mostActiveRegions;
			this.numReq = update.numReq;
		} else{
			this.numReq = this.numReq + update.numReq;
		}
		System.out.println("Coordinator combines update, "+this.numReq+" request");	
	}


	
	@Override
	public JSONObject getStats() {
		JSONObject json = new JSONObject();
		try {
			json.put(SERVICE_NAME, this.name);
			json.put(NUM_REQ, REPORT_THRESHOLD+1);
			json.put(HOST, mostActiveRegions);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		System.out.println("getStats:"+json.toString());
		
		return json;
	}

	@Override
	public void justReconfigured() {
		this.lastReconfiguredProfile = this.clone();		
	}

	@Override
	public void register(Request request, InetAddress sender, InterfaceGetActiveIPs nodeConfig) {
		Set<InetAddress> closest = RTTEstimator.getClosest(sender);
		String closestAddress = rearrange(closest);
		System.out.println("Closest replicas returned by RTTEstimator are:"+closest);
		
		if(mostActiveRegions == null ){
			mostActiveRegions = closestAddress;
			numReq = 0;
		} else if(! mostActiveRegions.equals(closestAddress)){
			mostActiveRegions = closestAddress;
			numReq = 0;			
		}else{
			numReq++;
			System.out.println(this+" Recived "+numReq+" requests");
		}
		
		System.out.println("register"+request+" "+sender);
		
	}

	@Override
	public void reset() {
		numReq = 0;
		mostActiveRegions = null;
	}
	
	@Override
	public ArrayList<InetAddress> shouldReconfigure(ArrayList<InetAddress> curActives, InterfaceGetActiveIPs nodeConfig) {
		if(this.numReq >= RECONFIGURATION_THRESHOLD){

			System.out.println("The most active region is "+mostActiveRegions);
			
			ArrayList<InetAddress> reconfiguredAddresses = new ArrayList<InetAddress>();
			String[] addr = mostActiveRegions.split(",");
			for (int i=0; i<addr.length; i++){
				try {
					reconfiguredAddresses.add( InetAddress.getByName(addr[i]) );
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
			System.out.println("Closest names are "+reconfiguredAddresses);
			
			this.numReq = 0;
			return reconfiguredAddresses;
		}else{
			return null;
		}
	}

	@Override
	public boolean shouldReport() {	
		/**
		 * Do not report on every request, report when
		 * a replica receives too many requests from a
		 * region. 
		 */
		if(numReq >= REPORT_THRESHOLD ){
			System.out.println("Should report to reconfigurator ...");
			return true;
		}
		return false;
		
	}
	
	private String rearrange(Set<InetAddress> address){
		assert( !address.isEmpty() );
		
		ArrayList<String> list = new ArrayList<String>();
		for (InetAddress addr:address){
			list.add(addr.getHostAddress().toString());
		}
		
		
		java.util.Collections.sort(list);
		
		String result = "";
		for(String addr:list){
			result = result + addr + ",";
		}
		result = result.substring(0, result.length()-1);
		System.out.println(result);
		return result;
	}
	
	/*
	protected static List<String> asSortedList() {
		Collection<String> c = PaxosConfig.getActives().keySet();
		List<String> list = new ArrayList<String>(c);
		java.util.Collections.sort(list);
		return list;
	}
	*/
}
