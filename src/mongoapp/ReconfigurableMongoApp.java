package mongoapp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.AppRequest.ResponseCodes;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author gaozy
 *
 */
public class ReconfigurableMongoApp extends AbstractReconfigurablePaxosApp<String> 
implements Replicable, Reconfigurable, ClientMessenger{
	
	final String DB_NAME = "mydb";
	
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
		
		//execute javascript sent from the client
		String[] command = {"mongo", "--eval", request.getValue()};	
		String response = "";
		try {
			response = executeCommand(command);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		sendResponse(request, response.equals("")? ResponseCodes.ACK.toString():response);
		return true;
	}
	
	@Override
	public boolean execute(Request request) {
		return execute(request, false);
	}
	
	private String executeCommand(String[] cmd) throws IOException, InterruptedException{
		Process proc;

		proc = Runtime.getRuntime().exec(cmd);
		proc.waitFor();
		
		StringBuilder builder = new StringBuilder();
		BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		String line = "";
		while ((line = br.readLine()) != null) {
		    builder.append(line+"\n");
		}
		br.close();
		
		return builder.toString();
	}
	
	@Override
	public String checkpoint(String name) {
		String[] cmd = {"mongoexport","--db", DB_NAME, "--collection", name, "--out", name+".json"};
		try {
			executeCommand(cmd);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		String state = LargeCheckpointer.createCheckpointHandle(name+".json");
		return state;
	}

	@Override
	public boolean restore(String name, String state) {
		if(state != null){
			LargeCheckpointer.restoreCheckpointHandle(state, name+".json");
			String[] cmd = {"mongoimport", "--db", DB_NAME, "--collection", name, "--file", name+".json"};
			try {
				executeCommand(cmd);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
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
