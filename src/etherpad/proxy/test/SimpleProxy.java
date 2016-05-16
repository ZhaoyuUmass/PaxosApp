package etherpad.proxy.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleProxy {
	final static String host = "52.26.182.238";
	final static int port = 9001;
	
//	protected static void read
	
	public static void main(String[] args) throws IOException{
		ServerSocket listener = new ServerSocket(8000);
		System.out.println("Start listening on port 8000 ...");
		try {
            while (true) {
                Socket clientSocket = listener.accept();
                
                try {
                	Socket socket = new Socket(host, port);
                			
            		PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            		out.println("GET / HTTP/1.0");
            		out.println(); 
            		out.flush();
            		
            		BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            		StringBuilder sb = new StringBuilder();
            		String line;
            		try {
            			while ((line = br.readLine()) != null) {
            				sb.append(line);
            			}

            		} catch (IOException e) {
            			e.printStackTrace();
            		} finally {
            			if (br != null) {
            				try {
            					br.close();
            				} catch (IOException e) {
            					e.printStackTrace();
            				}
            			}
            		}
            		
            		String response = sb.toString();
            		System.out.println("Response from etherpad:"+response);
                	
            		PrintWriter clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
            		clientOut.println(response);
            		clientOut.println();
            		clientOut.flush();
            		
//                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//                    out.println(new Date().toString());
                    
                    
                } finally {
                    clientSocket.close();
                }
            }
        }
        finally {
            listener.close();
        }
	}
}
