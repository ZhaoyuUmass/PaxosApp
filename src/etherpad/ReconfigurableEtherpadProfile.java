package etherpad;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.InterfaceGetActiveIPs;

/**
 * @author gaozy
 *
 */
public class ReconfigurableEtherpadProfile extends AbstractDemandProfile{
	private final static int RECONFIGURATION_THRESHOLD = 20;
	private final static int REPORT_THRESHOLD = 4;
	private final static String SERVICE_NAME = "service_name";
	private final static String NUM_REQ = "num_request";
	private final static String HOST = "host";
	
	private Integer numReq = 0;
	private ReconfigurableEtherpadProfile lastReconfiguredProfile = null;
	
	private FakeLatency latMap = new FakeLatency();
	private String mostActiveRegion = null;
	
	
	/**
	 * @param name
	 */
	public ReconfigurableEtherpadProfile(String name) {
		super(name);
	}
	
	/**
	 * @param nnp
	 */
	public ReconfigurableEtherpadProfile(ReconfigurableEtherpadProfile nnp) {
		super(nnp.name);
		this.numReq = nnp.numReq;
		this.mostActiveRegion = nnp.mostActiveRegion;
	}

	@Override
	public ReconfigurableEtherpadProfile clone() {
		return new ReconfigurableEtherpadProfile(this);	
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public ReconfigurableEtherpadProfile(JSONObject json) throws JSONException {
		super(json.getString(SERVICE_NAME));
		this.mostActiveRegion = json.getString(HOST);
		this.numReq = json.getInt(NUM_REQ);
	}
	
	
	@Override
	public void combine(AbstractDemandProfile dp) {
		ReconfigurableEtherpadProfile update = (ReconfigurableEtherpadProfile) dp;
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
			json.put(HOST, this.mostActiveRegion);
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
		
		AppRequest req = (AppRequest) request;
		String content = req.getValue();
		String host = content.split("|")[0];
		
		if(mostActiveRegion == null ){
			mostActiveRegion = host;
			numReq = 0;
		} else if(!mostActiveRegion.equals(host)){
			mostActiveRegion = host;
			numReq = 0;	
			
		}else{
			numReq++;
			System.out.println(this+" Recived "+numReq+" requests");
		}
		
		System.out.println("register"+request+" "+host);
		
	}

	@Override
	public void reset() {
		numReq = 0;
		mostActiveRegion = null;
	}
	
	@Override
	public ArrayList<InetAddress> shouldReconfigure(ArrayList<InetAddress> curActives, InterfaceGetActiveIPs nodeConfig) {
		if(this.numReq >= RECONFIGURATION_THRESHOLD){
			ArrayList<InetAddress> reconfiguredAddresses = new ArrayList<InetAddress>();
			System.out.println("The most active region is "+mostActiveRegion);
			
			ArrayList<String> names = latMap.getClosest(mostActiveRegion);
			
			System.out.println("Closest names are "+names);
			for (String name:names){
				reconfiguredAddresses.add(PaxosConfig.getActives().get(name).getAddress());
			}
			
			System.out.println("reconfigured address set is "+reconfiguredAddresses);
			
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
	
	protected static List<String> asSortedList() {
		Collection<String> c = PaxosConfig.getActives().keySet();
		List<String> list = new ArrayList<String>(c);
		java.util.Collections.sort(list);
		return list;
	}
}
