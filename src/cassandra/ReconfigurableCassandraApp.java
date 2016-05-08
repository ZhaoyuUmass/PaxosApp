package cassandra;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

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

/**
 * @author gaozy
 *
 */
public class ReconfigurableCassandraApp extends AbstractReconfigurablePaxosApp<String> 
implements Replicable, Reconfigurable, ClientMessenger {
	
	final String CQLSH = "/home/ubuntu/apache-cassandra-3.5/bin/cqlsh";
	final String KEYSPACE = "mykeyspace";
	final String delimiter = "\nDATA FROM HERE\n";
	
	Cluster cluster;
	Session session;
	
	/**
	 * 
	 */
	public ReconfigurableCassandraApp() {
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect(KEYSPACE);
	}
	
	@Override
	public boolean execute(Request request){
		return this.execute(request, false);
	}
	
	@Override
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
		long start = System.currentTimeMillis();		
		if (request.getServiceName() == null)
			return true; // no-op
		if (request.isStop()){
			System.out.println(this+": received stop msg "+request);
			return true;
		}
		
		String sql = request.getValue();
			
		session.execute(sql);
		
		this.sendResponse(request);
		System.out.println("Application execution time "+(System.currentTimeMillis() - start)+"ms");
		return true;
	}
	
	private String executeDump(String[] cmd){
		StringBuilder builder = new StringBuilder();
		try {
			Process proc = Runtime.getRuntime().exec(cmd);
			int processComplete = proc.waitFor();

			
			
			BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String line = "";
			while ((line = br.readLine()) != null) {
			    builder.append(line+"\n");
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return builder.toString();
	}
	
	@Override
	public String checkpoint(String name) {
		String state = "";

		String[] cmd1 = {CQLSH, "-e", "DESC KEYSPACE "+KEYSPACE};
		state = state + executeDump(cmd1);
		
		state = state + delimiter;
		
		String[] cmd2 = {CQLSH, "-e", "COPY mykeyspace."+name+" TO STDOUT;"};
		
		state = state + executeDump(cmd2);
		
		session.execute("DROP table "+name+";");
		
		return state;
	}
	
	private void executeRestore(String path, String[] cmd, String data) throws IOException, InterruptedException{
		File f = new File(path);
		
		FileWriter fw = new FileWriter(f);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(data);
		bw.flush();
		bw.close();		

		Process proc = Runtime.getRuntime().exec( cmd );
		proc.waitFor();
		
		f.delete();
	}
	
	
	@Override
	public boolean restore(String name, String state) {
		if (state != null){
			String[] schemaAndData = state.split(delimiter);
			
			String path = "schema.cql";
			
			String[] command1 = {CQLSH, "localhost", "-f", "schema.cql"};
			try{
				executeRestore(path, command1, schemaAndData[0]);
			} catch(Exception e){
				e.printStackTrace();
			}
			
			path = "data.cql";
			String[] command2 = {CQLSH, "-e", "COPY mykeyspace."+name+" FROM 'data.cql';"};
			try{
				executeRestore(path, command2, schemaAndData[1]);
			}catch(Exception e){
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
	
	private void sendResponse(AppRequest request) {
		// set to whatever response value is appropriate
		request.setResponse(ResponseCodes.ACK.toString());		
	}
	
}
