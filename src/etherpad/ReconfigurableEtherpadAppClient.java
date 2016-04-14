package etherpad;

import java.io.IOException;
import java.util.Set;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author gaozy
 *
 */
public class ReconfigurableEtherpadAppClient extends ReconfigurableAppClientAsync {
	
	static int NORMAL_REQUEST = 0;
	static int ERROR_REQUEST = 0;
	synchronized static void updateTotalRequest(Request request){
		if(request.getRequestType() != AppRequest.PacketType.DEFAULT_APP_REQUEST){
			ERROR_REQUEST++;
		} else {
			NORMAL_REQUEST++;
		}
	}
	
	/**
	 * @throws IOException
	 */
	public ReconfigurableEtherpadAppClient() throws IOException {
		super();
	}
	
	private static class Callback implements RequestCallback{

		@Override
		public void handleResponse(Request request) {
			System.out.println("After stop received: "+request+" "+request.getRequestType()+" "+request.getClass());
			ReconfigurableEtherpadAppClient.updateTotalRequest(request);
		}
		
	}
	
	@SuppressWarnings("unused")
	private static void sendRequestAfterStop(ReconfigurableEtherpadAppClient client, String name) throws IOException{
		for (String activeName:PaxosConfig.getActives().keySet()) {
			System.out.println("Active: "+activeName);
			client.sendRequest(new AppRequest(name, "100",
				AppRequest.PacketType.DEFAULT_APP_REQUEST, false), 
				PaxosConfig.getActives().get(activeName), 
				new Callback());
		}
	}
	
	private static void sendTestReqeust(ReconfigurableEtherpadAppClient client, String name) throws IOException{
		client.sendRequest(new AppRequest(name, "100",
				AppRequest.PacketType.DEFAULT_APP_REQUEST, false), 
				new RequestCallback(){
					@Override
					public void handleResponse(Request response) {
						System.out.println("The response is " +response);
						
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						/*
						try {
							sendRequestAfterStop(client, name);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						*/
					}
			
				});
		
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
	
	public static void main(String[] args) throws IOException, InterruptedException{
		final ReconfigurableEtherpadAppClient client = new ReconfigurableEtherpadAppClient();
		// group name is also the pad name
		final String name = ReconfigurableEtherpadApp.class.getSimpleName()+0;
		
		
		System.out.println("Actives are :"+PaxosConfig.getActives());
		client.sendRequest(new CreateServiceName(name, ""),
				new RequestCallback() {

					@Override
					public void handleResponse(Request request) {
						System.out.println("Received:"+request);
						/*
						try {
							sendTestReqeust(client, name);
						} catch (IOException e) {
							e.printStackTrace();
						}
						*/
					}
				});
		
		Thread.sleep(2000);
		
		System.out.println("Experiment is done, reconfiguration acts as expected!");
		
		System.exit(0);
	}
}
