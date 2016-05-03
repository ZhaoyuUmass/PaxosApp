package scratch;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import com.mysql.jdbc.Driver;

public class MySQL_TEST {
	
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"; 
	static final String DB_NAME = "feedback";
	static final String DB_URL = "jdbc:mysql://localhost/"+DB_NAME;

	static final String USER = "root";
	static final String PASSWORD = "sd86787439";
	
	static final String filePath = "ReconfigurableMySQLApp0.sql";
	static Connection conn = null;
	static Statement stmt = null;
	
	public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException, InterruptedException{
		Class.forName("com.mysql.jdbc.Driver");
		String command = "mysql -u " + USER + " --password=" + PASSWORD
				 + " feedback < " + filePath;
		Process runtimeProcess = Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", command });
		int processComplete = runtimeProcess.waitFor();
		
		conn = DriverManager.getConnection(DB_URL,USER,PASSWORD);
		stmt = conn.createStatement();
		
		/*
		String sql = "DROP TABLE IF EXISTS salary";
		stmt.execute(sql);
		
		sql = "CREATE TABLE IF NOT EXISTS salary (id INTEGER not NULL, salary INTEGER, PRIMARY KEY ( id ))";
		stmt.execute(sql);
		
		System.out.println("created table!");
		
		sql = "INSERT INTO salary (id, salary) VALUES (0, 0)";
		stmt.execute(sql);
		*/
	}
}
