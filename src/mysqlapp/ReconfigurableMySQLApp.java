package mysqlapp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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

/**
 * @author gaozy
 *
 */
public class ReconfigurableMySQLApp extends AbstractReconfigurablePaxosApp<String> 
implements Replicable, Reconfigurable, ClientMessenger{
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"; 
	static final String DB_NAME = "feedback";
	static final String DB_URL = "jdbc:mysql://localhost/"+DB_NAME;

	static final String USER = "root";
	static final String PASSWORD = "sd86787439";
	
	Connection conn = null;
	Statement stmt = null;
		
	/**
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * 
	 */
	public ReconfigurableMySQLApp() throws ClassNotFoundException, SQLException{
	
		Class.forName("com.mysql.jdbc.Driver");
		conn = DriverManager.getConnection(DB_URL,USER,PASSWORD);
		stmt = conn.createStatement();
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
		if (request.getServiceName() == null)
			return true; // no-op
		if (request.isStop()){
			return true;
		}
		
		String sql = request.getValue();
		
		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		this.sendResponse(request);
		return true;
	}
	
	@Override
	public String checkpoint(String name) {
		StringBuilder builder = new StringBuilder();
		builder.append("USE "+DB_NAME+";\n");
		
		String executeCmd = "mysqldump -u" + USER + " --password=" + PASSWORD + " " + DB_NAME+" "+name;
		try {
			Process proc = Runtime.getRuntime().exec(executeCmd);
			int processComplete = proc.waitFor();
			// process must exit normally
			assert(processComplete  == 0);
			
			BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String line = "";
			while ((line = br.readLine()) != null) {
			    builder.append(line+"\n");
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		// drop table first
		try {
			stmt.execute("DROP TABLE IF EXISTS "+name);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return builder.toString();
	}

	@Override
	public boolean restore(String name, String state) {
		
		if (state != null){
			/**
			 * write into a file and load it into mysql
			 */
			String path = name+".sql";
			File f = new File(path);
			try {				
				FileWriter fw = new FileWriter(f);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(state);
				bw.flush();
				bw.close();				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			String command = "mysql -u" + USER + " --password=" + PASSWORD + " "+DB_NAME+" < " + path;
			try {
				Process proc = Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", command });
				proc.waitFor();

			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
			//f.delete();
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
	public void setClientMessenger(SSLMessenger<?, JSONObject> msgr) {
		// Do nothing	
	}

	private void sendResponse(AppRequest request) {
		// set to whatever response value is appropriate
		request.setResponse(ResponseCodes.ACK.toString() );
	}
}