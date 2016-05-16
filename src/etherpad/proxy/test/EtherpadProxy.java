package etherpad.proxy.test;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpServer;

/**
 * @author gaozy
 *
 */
public class EtherpadProxy {	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException{
		
		HttpServer server = HttpServer.create(new InetSocketAddress(8000), 1000);
		server.createContext("/", new ProxyHandler());
		server.setExecutor(null);
		server.start();
		System.out.println("Proxy is started ... ");
		/*
		HashMap map = epConnection.post("createGroup");
		String response = "{";
		for (Object key:map.keySet()){
			response = response + key.toString()+":"+map.get(key)+",";
		}
		response = response.substring(0, response.length()-2);
		response += "}";
		System.out.println(response);
		//epConnection.post(apiMethod)
		 * 
		 */
	}
}
