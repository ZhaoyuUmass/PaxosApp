package etherpad.proxy.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class ProxyHandler implements HttpHandler{
	final static String host = "52.26.182.238";
	final static int port = 9001;
	
	final static String hostName = "http://52.26.182.238:" + 9001;
	final static String apiKey = "477304aadcaafc1ad3665e700b99583a4c21480b3e04939c3c6cd903ba72cd28";
	final static String DEFAULT_API_VERSION = "1.2.1";
	
	//EtherpadConnection epConnection = new EtherpadConnection(hostName, apiKey, DEFAULT_API_VERSION);
	
	@Override
	public void handle(HttpExchange t) throws IOException {
		System.out.println(t.getRequestBody()+" "+t.getHttpContext());
		/*
		InputStream is = t.getRequestBody();
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		String line;
		try {

			br = new BufferedReader(new InputStreamReader(is));
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
		*/

		System.out.println("Received the request");

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
		
		t.sendResponseHeaders(200, response.length());
		OutputStream os = t.getResponseBody();
		os.write(response.getBytes());
		os.close();
		System.out.println("Response to send is: "+t.getResponseBody());
	}


}
