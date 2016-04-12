package etherpad;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.AppRequest.ResponseCodes;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import net.gjerull.etherpad.client.EPLiteClient;

/**
 * @author gaozy
 *
 */
public class ReconfigurableEtherpadApp extends AbstractReconfigurablePaxosApp<String> 
	implements Replicable, Reconfigurable, ClientMessenger {
	
	final static int port = 9001;
	final static String hostName = "http://127.0.0.1:" + port;
	final static String apiKey = "7c1a3ced07e809703dcaf714426843356d3af35073d4a991ce4606e34f818f16";
	final static EPLiteClient client = new EPLiteClient(hostName, apiKey);
	final static String delimiter = ",";
	
	private final HashMap<String, String> cache = new HashMap<String, String>();	
	
	
	
	private boolean processRequest(AppRequest request,
			boolean doNotReplyToClient) {
		
		this.sendResponse(request);
		if (request.getServiceName() == null)
			return true; // no-op
		if (request.isStop()){
			System.out.println(this+": received stop msg "+request);
			return true;
		}
				
		String name = request.getServiceName();
		String value = request.getValue();
		
		int pos = value.indexOf("|");
		//assert(pos != -1);
		System.out.println("The position is "+pos);
		value = value.substring(pos+1);
		
		return parseRequest(name, value);
	}

	private boolean parseRequest(String padName, String value){
		
		//String content = client.getText(padName).get("text").toString();
		//content = content + value;
		client.setText(padName, value);
		return true;
	}
	
	@Override
	public String checkpoint(String name) {
		String data = client.getText(name).get("text").toString();
		System.out.println("checkpoint data:"+data);
		return data != null? data : null;
	}

	@Override
	public boolean restore(String padName, String state) {
				
		System.out.println(this+":restore "+padName+" "+state);
		String data = null;
		
		HashMap<String, Object> padMap = client.listAllPads();
		List<String> padIDs = (List<String>) padMap.get("padIDs");
		for(String item:padIDs){
			System.out.println("Existing pad:"+item);
		}
		
		HashMap<String, Object> map = client.getText(padName);
		if(map.containsKey("text")){
			data = map.get("text").toString();
		}
		
		if (state == null && data != null){
			//client.deletePad(padName);
			client.createPad(padName);
		}
		if (state != null){
			client.createPad(padName, state);
		}
		
		System.out.println(this+": restore state has been restored "+state);
		
		return true;
	}

	@Override
	public boolean execute(Request request) {		
		return execute(request, false);
	}

	@Override
	public Request getRequest(String stringified) throws RequestParseException {
		try {
			return NoopApp.staticGetRequest(stringified);
		} catch (RequestParseException | JSONException e) {
			// do nothing by design
		}
		return null;
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return NoopApp.staticGetRequestTypes();
	}
	
	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) {
		if (request.toString().equals(Request.NO_OP)){
			return true;
		}
				
		switch ((AppRequest.PacketType) (request.getRequestType())) {
		case DEFAULT_APP_REQUEST:
			return processRequest((AppRequest) request, doNotReplyToClient);
		default:
			break;
		}		
		System.out.println("The demo program should never come here!");
		return false;
	}
	
	
	private String myID;
	
	@Override
	public void setClientMessenger(SSLMessenger<?, JSONObject> msgr) {
		this.myID = msgr.getMyID().toString();		
	}	
	
	public String toString(){
		return this.getClass().getSimpleName()+myID;
	}
	
	private void sendResponse(AppRequest request) {
		// set to whatever response value is appropriate
		request.setResponse(ResponseCodes.ACK.toString() );
	}
}
