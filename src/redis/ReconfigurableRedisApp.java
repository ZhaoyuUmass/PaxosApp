package redis;

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
import redis.clients.jedis.Jedis;

/**
 * @author gaozy
 *
 */
public class ReconfigurableRedisApp extends AbstractReconfigurablePaxosApp<String> 
implements Replicable, Reconfigurable, ClientMessenger{
	Jedis jedis;
	
	ReconfigurableRedisApp(){
		jedis = new Jedis("localhost");
	}
	
	public boolean execute(Request request, boolean doNotReplyToClient) {
		
		if (request.toString().equals(Request.NO_OP)){
			return true;
		}
				
		switch ((AppRequest.PacketType) (request.getRequestType())) {
		case DEFAULT_APP_REQUEST:
			return processRequest((AppRequest) request);
		default:
			break;
		}		
		return false;
	}
	
	private boolean processRequest(AppRequest request) {
		if (request.getServiceName() == null)
			return true; // no-op
		if (request.isStop()){
			return true;
		}
		
		String[] params = request.getValue().split(",");
		switch(params[0]){
		case "GET":
			String response = jedis.get(request.getServiceName()).toString();
			this.sendResponse(request, response);
			break;
		case "SET":
			jedis.set(request.getServiceName(), params[1]);
			this.sendResponse(request, ResponseCodes.ACK.toString());
			break;
		}
		
		return true;
	}
	
	@Override
	public boolean execute(Request request) {
		return execute(request, false);
	}

	@Override
	public String checkpoint(String name) {
		String state = jedis.get(name);
		jedis.del(name);
		return state;
	}

	@Override
	public boolean restore(String name, String state) {
		if(state != null){
			jedis.set(name, state);
		}
		return true;
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
	public void setClientMessenger(SSLMessenger<?, JSONObject> arg0) {
		// do nothing
	}
	
	private void sendResponse(AppRequest request, String response) {
		// set to whatever response value is appropriate
		request.setResponse(response);		
	}
}
