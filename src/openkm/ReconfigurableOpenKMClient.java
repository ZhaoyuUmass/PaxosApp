package openkm;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
public class ReconfigurableOpenKMClient extends ReconfigurableAppClientAsync {
	final static String serviceName = ReconfigurableOpenKMApp.class.getSimpleName()+0;	
	private final static int NUM_THREAD = 10;
	private final static int TIMEOUT = 1000;
	
	private static int NUM_REQ = 1;
	
	private static int REQ_ID = 0;
	synchronized static int getNextID(){
		return REQ_ID++;
	}
	
	private static SecureRandom random = new SecureRandom();
	
	private static String getRandomString() {
		return new BigInteger(128, random).toString(32);
	}
	
	private static ReconfigurableOpenKMClient client;	
	private static ThreadPoolExecutor executorPool = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS, 
    		new LinkedBlockingQueue<Runnable>(), new MyThreadFactory());
	
	static ArrayList<Long> latencies = new ArrayList<Long>();
	static int received = 0;
	static int timeout = 0;
	static synchronized void updateLatency(long latency){
		latencies.add(latency);
	}
	
	/*
	 * OpenKM related parameters
	 */
	protected final static String delimiter = ",";
	protected final static String root = "/okm:root/";
	protected final static String fdelimiter = "#";
	
	static class MyThreadFactory implements ThreadFactory {
		
    	public Thread newThread(Runnable r) {
    		Thread t = new Thread(r);
    	    return t;
    	}
	}
	
	private static class RequestRunnable implements Callable<Boolean>{
		Object obj = new Object();
		String content = "";
		
		RequestRunnable(String content){
			this.content = content;
		}
		
		@Override
		public Boolean call() throws Exception {
			
			try {
				System.out.println("Send request "+received+" to "+serviceName);
				client.sendRequest(new AppRequest(serviceName, this.content,
						AppRequest.PacketType.DEFAULT_APP_REQUEST, false)
						, new OpenKMCallback(System.currentTimeMillis(), obj));
			} catch (IOException e) {
				e.printStackTrace();
			}
			synchronized(obj){
				received++;
				obj.wait();			
			}
			return true;
		}
		
	}
	
	/**
	 * @throws IOException
	 */
	public ReconfigurableOpenKMClient() throws IOException {
		super();	
	}
	
	private static class OpenKMCallback implements RequestCallback{
		private long initTime;
		private Object obj;
		
		OpenKMCallback(long initTime, Object obj){
			this.initTime = initTime;
			this.obj = obj;
		}
		
		@Override
		public void handleResponse(Request response) {
			
			long eclapsed = System.currentTimeMillis() - this.initTime;
			
			if(response.getRequestType() != AppRequest.PacketType.DEFAULT_APP_REQUEST){
				updateLatency(0);
				System.out.println("Latency of request"+received +":"+eclapsed+"ms."+response+" "+response.getRequestType());
			}else{
				updateLatency(eclapsed);
				System.out.println("Latency of request"+received +":"+eclapsed+"ms");
			}
			synchronized(obj){
				obj.notify();
			}
		}
		
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
	
	/**
	 * curl -u okmAdmin:admin -H Accept:application/xml -X POST -H Content-Type:application/json -d /okm:root/newfolder http://localhost:8080/OpenKM/services/rest/folder/createSimple 
	 */
	protected static String createFolderCommand(String folder){
		String cmd = "curl";
		cmd = cmd + delimiter + "-u";
		cmd = cmd + delimiter + "okmAdmin:admin";
		cmd = cmd + delimiter + "-H";
		cmd = cmd + delimiter + "Accept: application/xml";
		cmd = cmd + delimiter + "-X";
		cmd = cmd + delimiter + "POST";
		cmd = cmd + delimiter + "-H";
		cmd = cmd + delimiter + "Content-Type: application/json";
		cmd = cmd + delimiter + "-d";
		cmd = cmd + delimiter + root + folder;
		cmd = cmd + delimiter + "http://localhost:8080/OpenKM/services/rest/folder/createSimple";
		return cmd;
	}
	
	protected static String deleteFolderCommand(String uuid){
		String cmd ="curl";
		cmd = cmd + delimiter + "-u";
		cmd = cmd + delimiter + "okmAdmin:admin";
		cmd = cmd + delimiter + "-H";
		cmd = cmd + delimiter + "Accept: application/xml";
		cmd = cmd + delimiter + "-X";
		cmd = cmd + delimiter + "DELETE";
		cmd = cmd + delimiter + "http://localhost:8080/OpenKM/services/rest/folder/delete?fldId="+uuid;
		return cmd;
	}
	
	/**
	 * curl -u okmAdmin:admin -H "Accept: application/xml" -X POST -F docPath=/okm:root/test/1.txt -F content=hello\ world http://localhost:8080/OpenKM/services/rest/document/createSimple
	 */
	protected static String createDocumentCommand(String docName, String content){
		String cmd = "curl";
		cmd = cmd + delimiter + "-u";
		cmd = cmd + delimiter + "okmAdmin:admin";
		cmd = cmd + delimiter + "-H";
		cmd = cmd + delimiter + "Accept: application/xml";
		cmd = cmd + delimiter + "-X";
		cmd = cmd + delimiter + "POST";
		cmd = cmd + delimiter + "-F";
		cmd = cmd + delimiter + "docPath="+docName;
		cmd = cmd + delimiter + "-F";
		cmd = cmd + delimiter + "content="+content;
		cmd = cmd + delimiter + "http://localhost:8080/OpenKM/services/rest/document/createSimple";
		
		return cmd;
	}
	
	
	/**
	 * curl -u okmAdmin:admin -H "Accept: application/xml" -X GET http://localhost:8080/OpenKM/services/rest/repository/getNodeUuid?nodePath=okm:root/test/1.txt
	 */
	protected static String getUuidCommand(String path){
		String cmd = "curl";
		cmd = cmd + delimiter + "-u";
		cmd = cmd + delimiter + "okmAdmin:admin";
		cmd = cmd + delimiter + "-H";
		cmd = cmd + delimiter + "Accept: application/xml";
		cmd = cmd + delimiter + "-X";
		cmd = cmd + delimiter + "GET"; 
		cmd = cmd + delimiter + "http://localhost:8080/OpenKM/services/rest/repository/getNodeUuid?nodePath=";
		cmd = cmd + root + path;
		
		return cmd;
	}
	
	/**
	 * curl -u okmAdmin:admin -H "Accept: application/xml" -X GET http://localhost:8080/OpenKM/services/rest/document/getChildren?fldId=08936afa-6ded-44d5-818d-002fe5f0079d
	 * @param uuid
	 * @return
	 */
	public static String getDocumentChildrenCommand(String uuid){
		String cmd = "curl";
		cmd = cmd + delimiter + "-u";
		cmd = cmd + delimiter + "okmAdmin:admin";
		cmd = cmd + delimiter + "-H";
		cmd = cmd + delimiter + "Accept: application/xml";
		cmd = cmd + delimiter + "-X";
		cmd = cmd + delimiter + "GET"; 
		cmd = cmd + delimiter + "http://localhost:8080/OpenKM/services/rest/document/getChildren?fldId="+uuid;
		return cmd;
	}
	
	/**
	 * http://localhost:8080/OpenKM/services/rest/document/getContent?docId=22b350c7-90fe-4fa2-9fbc-e637ad2f10ef
	 * @param client
	 */
	protected static String getDocumentContentCommand(String uuid){
		String cmd = "curl";
		cmd = cmd + delimiter + "-u";
		cmd = cmd + delimiter + "okmAdmin:admin";
		cmd = cmd + delimiter + "http://localhost:8080/OpenKM/services/rest/document/getContent?docId="+uuid;
		return cmd;
	}
	
	private static void sendOpenKMReqeust(ReconfigurableOpenKMClient client){
		// Create folder
		String command = createFolderCommand(serviceName);
		
		Future<Boolean> future = executorPool.submit(new RequestRunnable(command));
		try {
			future.get(TIMEOUT, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
		}
		
		// Create document
		System.out.println("Start sending "+NUM_REQ+" requests...");
		for (int i=0; i<NUM_REQ; i++){
			command = createDocumentCommand(root+serviceName+"/"+getRandomString()+".txt", "helloworld");
			future = executorPool.submit(new RequestRunnable(command));
			
			try {
				future.get(TIMEOUT, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				e.printStackTrace();
			}
		}
		
		long totalLatency = 0;
		String response = "";
		for (long lat:latencies){
			totalLatency += lat;
			response += lat+",";
		}
		
		if(response.length() > 1)
			response = response.substring(0, response.length()-1);
		
		System.out.println("Sent "+NUM_REQ+" requests, received "+(received-timeout)+" requests and "+ timeout + " requests timed out"
				+ ". The average latency is "+totalLatency/(received-timeout)+"ms");
		System.out.println("RESPONSE:"+response);
				
		System.exit(0);
		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException{
		NUM_REQ = Integer.parseInt(args[0]);
		
		client = new ReconfigurableOpenKMClient();
		
		System.out.println("Actives are :"+PaxosConfig.getActives());
		client.sendRequest(new CreateServiceName(serviceName, 
				""),
				new RequestCallback() {

					@Override
					public void handleResponse(Request request) {
						System.out.println("Received:"+request);
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
						sendOpenKMReqeust(client);					
					}
				});
		
		while(latencies.size() < NUM_REQ){
			System.out.println("received : "+latencies.size());
			Thread.sleep(1000);
		}
		
		System.exit(0);
		
	}
}
