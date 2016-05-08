package cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
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
public class ReconfigurableCassandraClient extends ReconfigurableAppClientAsync{
	
	final static String serviceName = ReconfigurableCassandraApp.class.getSimpleName()+0;	
	private final static int NUM_THREAD = 10;
	private final static int TIMEOUT = 1000;
	
	private static int NUM_REQ = 1;
	
	private static int REQ_ID = 0;
	synchronized static int getNextID(){
		return REQ_ID++;
	}
	private static Random rand = new Random();
	
	private static ReconfigurableCassandraClient client;	
	private static ThreadPoolExecutor executorPool = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS, 
    		new LinkedBlockingQueue<Runnable>(), new MyThreadFactory());
	
	static ArrayList<Long> latencies = new ArrayList<Long>();
	static int received = 0;
	static int timeout = 0;
	static synchronized void updateLatency(long latency){
		latencies.add(latency);
	}
	
	/**
	 * @throws IOException
	 */
	public ReconfigurableCassandraClient() throws IOException {
		super();
	}
	
	static class MyThreadFactory implements ThreadFactory {
		
    	public Thread newThread(Runnable r) {
    		Thread t = new Thread(r);
    	    return t;
    	}
	}
	
	
	private static class RequestRunnable implements Callable<Boolean>{
		Object obj = new Object();
		String sql = "";
		
		RequestRunnable(String sql){
			this.sql = sql;
			System.out.println("SQL command is:"+sql);
		}
		
		@Override
		public Boolean call() throws Exception {
			
			try {
				System.out.println("Send request "+getNextID()+" to "+serviceName);
				client.sendRequest(new AppRequest(serviceName, this.sql,
						AppRequest.PacketType.DEFAULT_APP_REQUEST, false)
						, new CassandraCallback(System.currentTimeMillis(), obj));
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
	
	private static class CassandraCallback implements RequestCallback{
		private long initTime;
		private Object obj;
		
		CassandraCallback(long initTime, Object obj){
			this.initTime = initTime;
			this.obj = obj;
		}
		
		@Override
		public void handleResponse(Request response) {
			
			long eclapsed = System.currentTimeMillis() - this.initTime;
			
			if(response.getRequestType() != AppRequest.PacketType.DEFAULT_APP_REQUEST){
				updateLatency(0);
				System.out.println("Latency of request"+REQ_ID +":"+eclapsed+"ms."+response+" "+response.getRequestType());
			}else{
				updateLatency(eclapsed);
				System.out.println("Latency of request"+REQ_ID +":"+eclapsed+"ms");
			}
			synchronized(obj){
				obj.notify();
			}
		}
		
	}
	
	
	private static void sendCassandraReqeust(ReconfigurableCassandraClient client){
		String drop_table = "DROP TABLE IF EXISTS "+serviceName+";";
		Future<Boolean> future = executorPool.submit(new RequestRunnable(drop_table));
		try {
			future.get(TIMEOUT, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
		}
		
		String create_table = "CREATE TABLE "+serviceName+" (id int PRIMARY KEY, salary int);";
		future = executorPool.submit(new RequestRunnable(create_table));
		try {
			future.get(TIMEOUT, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
		}
		
		String insert_record = "INSERT INTO "+serviceName+" (id, salary) VALUES (0, 1000);";
		future = executorPool.submit(new RequestRunnable(insert_record));
		try {
			future.get(TIMEOUT, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
		}
		
		System.out.println("Start sending "+NUM_REQ+" requests...");
		for (int i=0; i<NUM_REQ; i++){
			int salary = rand.nextInt(1000);
			String update_record = "UPDATE "+serviceName+" SET salary="+salary+" WHERE id=0;";
			future = executorPool.submit(new RequestRunnable(update_record));
			
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
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException{
		// CREATE TABLE IF NOT EXISTS salary (id int PRIMARY KEY, salary int)
		// INSERT INTO salary (id, salary) VALUES (0, 1000);
		// UPDATE salary SET salary=100 WHERE id=0;
		
		// Runtime: /Users/gaozy/apache-cassandra-3.5/bin/cqlsh -e "COPY mykeyspace.salary TO STDOUT;"
		// Runtime: ./apache-cassandra-3.5/bin/cqlsh -e "DESC KEYSPACE mykeyspace" > user_schema.cql
		// Runtime: ./apache-cassandra-3.5/bin/cqlsh localhost -f user_schema.cql
		// Runtime: /Users/gaozy/apache-cassandra-3.5/bin/cqlsh -e "COPY mykeyspace.salary FROM 'test.cql';"
		
		NUM_REQ = Integer.parseInt(args[0]);
		
		client = new ReconfigurableCassandraClient();
		
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
						

						sendCassandraReqeust(client);
						
						
					}
				});
		
		while(latencies.size() < NUM_REQ){
			System.out.println("received : "+latencies.size());
			Thread.sleep(1000);
		}
		
		System.exit(0);
	}
}
