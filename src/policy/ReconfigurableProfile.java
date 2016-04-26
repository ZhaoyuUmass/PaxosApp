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
	
	private Integer numReq = 0;
	private ReconfigurableProfile lastReconfiguredProfile = null;
	
	private InetAddress mostActiveRegion = null;
	
	
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
		this.mostActiveRegion = nnp.mostActiveRegion;
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
		try {
			this.mostActiveRegion = InetAddress.getByName(json.getString(HOST));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		this.numReq = json.getInt(NUM_REQ);
	}
	
	
	@Override
	public void combine(AbstractDemandProfile dp) {
		ReconfigurableProfile update = (ReconfigurableProfile) dp;
		if( ! update.mostActiveRegion.equals(this.mostActiveRegion)){
			this.mostActiveRegion = update.mostActiveRegion;
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
			json.put(HOST, this.mostActiveRegion.getHostAddress());
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
		if(mostActiveRegion == null ){
			mostActiveRegion = sender;
			numReq = 0;
		} else if(mostActiveRegion == sender){
			mostActiveRegion = sender;
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
		mostActiveRegion = null;
	}
	
	@Override
	public ArrayList<InetAddress> shouldReconfigure(ArrayList<InetAddress> curActives, InterfaceGetActiveIPs nodeConfig) {
		if(this.numReq >= RECONFIGURATION_THRESHOLD){

			System.out.println("The most active region is "+mostActiveRegion);
			Set<InetAddress> addresses = null;
			try{
				addresses = RTTEstimator.getClosest(mostActiveRegion);
			}catch(Exception e){
				e.printStackTrace();
			}
			System.out.println("result get from getClosest is:"+addresses);
			ArrayList<InetAddress> reconfiguredAddresses = new ArrayList<InetAddress>(addresses);			
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
	
	/*
	protected static List<String> asSortedList() {
		Collection<String> c = PaxosConfig.getActives().keySet();
		List<String> list = new ArrayList<String>(c);
		java.util.Collections.sort(list);
		return list;
	}
	*/
}
