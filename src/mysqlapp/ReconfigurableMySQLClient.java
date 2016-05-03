package mysqlapp;

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
import mysqlapp.ReconfigurableMySQLApp;

/**
 * @author gaozy
 *
 */
public class ReconfigurableMySQLClient extends ReconfigurableAppClientAsync {
	
	final static String serviceName = ReconfigurableMySQLApp.class.getSimpleName()+0;	
	private final static int NUM_THREAD = 10;
	private final static int TIMEOUT = 1000;
	
	private static int NUM_REQ = 1;
	
	private static int REQ_ID = 0;
	synchronized static int getNextID(){
		return REQ_ID++;
	}
	private static Random rand = new Random();
	
	private static ReconfigurableMySQLClient client;	
	private static ThreadPoolExecutor executorPool = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS, 
    		new LinkedBlockingQueue<Runnable>(), new MyThreadFactory());
	
	static ArrayList<Long> latencies = new ArrayList<Long>();
	static int received = 0;
	static int timeout = 0;
	static synchronized void updateLatency(long latency){
		latencies.add(latency);
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
				System.out.println("Send request "+received+" to "+serviceName);
				client.sendRequest(new AppRequest(serviceName, this.sql,
						AppRequest.PacketType.DEFAULT_APP_REQUEST, false)
						, new MySQLCallback(System.currentTimeMillis(), obj));
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
	public ReconfigurableMySQLClient() throws IOException {
		super();
	}
	
	private static class MySQLCallback implements RequestCallback{
		private long initTime;
		private Object obj;
		
		MySQLCallback(long initTime, Object obj){
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
	
	
	private static void sendMySQLReqeust(ReconfigurableMySQLClient client){
		String drop_table = "DROP TABLE IF EXISTS salary;";
		Future<Boolean> future = executorPool.submit(new RequestRunnable(drop_table));
		try {
			future.get(TIMEOUT, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
		}
		
		String create_table = "CREATE TABLE salary (id INTEGER not NULL, salary INTEGER, PRIMARY KEY ( id ));";
		future = executorPool.submit(new RequestRunnable(create_table));
		try {
			future.get(TIMEOUT, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
		}
		
		System.out.println("Start sending "+NUM_REQ+" requests...");
		for (int i=0; i<NUM_REQ; i++){
			int id = getNextID();
			int salary = rand.nextInt(1000);
			String insert_record = "INSERT INTO salary (id, salary) VALUES ("+id+","+salary+")";
			future = executorPool.submit(new RequestRunnable(insert_record));
			
			try {
				future.get(TIMEOUT, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				e.printStackTrace();
			}
		}
		
		
		System.out.println("Experiment is done, reconfiguration acts as expected!");
		
		System.exit(0);
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
		NUM_REQ = Integer.parseInt(args[0]);
		
		client = new ReconfigurableMySQLClient();
		
		System.out.println("Actives are :"+PaxosConfig.getActives());
		client.sendRequest(new CreateServiceName(serviceName, 
				"CREATE TABLE salary (id INTEGER not NULL, salary INTEGER, PRIMARY KEY ( id ));"),
				new RequestCallback() {

					@Override
					public void handleResponse(Request request) {
						System.out.println("Received:"+request);
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
						

						sendMySQLReqeust(client);
						
						
					}
				});
		
		while(latencies.size() < NUM_REQ){
			System.out.println("received : "+latencies.size());
			Thread.sleep(1000);
		}
		
		System.exit(0);
		
	}
}
