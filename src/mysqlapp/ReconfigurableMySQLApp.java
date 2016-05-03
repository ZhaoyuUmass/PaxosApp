package mysqlapp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
	
	//ArrayList<String> tables = new ArrayList<String>();
	
	/**
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * 
	 */
	public ReconfigurableMySQLApp() throws ClassNotFoundException, SQLException{
	
		Class.forName("com.mysql.jdbc.Driver");
		conn = DriverManager.getConnection(DB_URL,USER,PASSWORD);
		stmt = conn.createStatement();
		
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet res = meta.getTables(null, null, null, null);
		while(res.next()){
			//tables.add(res.getString("TABLE_NAME"));
		}
		
		System.out.println(" >>>>>>>>>>>>>>>>>> Connection initialized!");
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
		
		System.out.println(this+": received "+request);
		
		String sql = request.getValue();
		
		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		this.sendResponse(request);
		System.out.println("Application execution time "+(System.currentTimeMillis() - start)+"ms");
		return true;
	}
	
	@Override
	public String checkpoint(String name) {
		StringBuilder builder = new StringBuilder();
		String executeCmd = "mysqldump -u" + USER + " --password=" + PASSWORD + " " + DB_NAME+" "+name;
		try {
			Process proc = Runtime.getRuntime().exec(executeCmd);
			int processComplete = proc.waitFor();
			// process must exit normally
			assert(processComplete  == 0);
			
			BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String line = "";
			while ((line = br.readLine()) != null) {
			    builder.append(line);
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
			
			String executeCmd = "mysql -u" + USER + " --password=" + PASSWORD + " < " + path;
			try {
				Process proc = Runtime.getRuntime().exec(executeCmd);
				int processComplete = proc.waitFor();
				System.out.println("database path has been restored.");
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
