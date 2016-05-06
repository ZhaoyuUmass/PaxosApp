package scratch;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import openkm.ReconfigurableOpenKMClient;

/**
 * @author gaozy
 *
 */
public class OpenKM_TEST {
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParserConfigurationException 
	 * @throws SAXException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, InterruptedException {
         	
            //String uuid = "b99730c1-4ce5-4758-af89-062e6f0d433b";
            /*
            String nodePath = "okm:root/test";
            URL url = new URL("http://localhost:8080/OpenKM/services/rest/repository/getNodeUuid?nodePath="+ nodePath);
            //URL url = new URL("http://localhost:8080/OpenKM/services/rest/document/getChildren?fldId=" + uuid);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/xml");

			Authenticator.setDefault(new Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication("okmAdmin", "admin"
							.toCharArray());
				}
			});

			if (conn.getResponseCode() == 200) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						(conn.getInputStream())));
				String output;

				while ((output = br.readLine()) != null) {
					uuid = output;
				}
			} else {
				System.err.println("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}
			
			System.out.println("UUID is "+uuid);
            */
		
			
			
			/*
			String u = "http://localhost:8080/OpenKM/services/rest/document/getChildren?fldId=" + uuid;
			//String u = "http://localhost:8080/OpenKM/services/rest/folder/getChildren?fldId=" + uuid;
			System.out.println("URL is "+u);
			URL url1 = new URL(u);
			HttpURLConnection conn1 = (HttpURLConnection) url1.openConnection();
            conn1.setRequestMethod("GET");
            conn1.setRequestProperty("Accept", "application/xml");
 
            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication("okmAdmin", "admin".toCharArray());
                }
            });
            
           
            StringBuilder sb = new StringBuilder();
            
            if (conn1.getResponseCode() == 200) {
            	 BufferedReader br = new BufferedReader(new InputStreamReader((conn1.getInputStream())));
                 System.out.println("Output from Server .... \n");
                 String output;

                 while ((output = br.readLine()) != null) {
                     sb.append(output+"\n");
                 }
                 
                 System.out.println(sb.toString());
                 
                 DocumentBuilderFactory factory =
                 DocumentBuilderFactory.newInstance();
                 DocumentBuilder builder = factory.newDocumentBuilder();
                 
                 ByteArrayInputStream input =  new ByteArrayInputStream(
                		   sb.toString().getBytes("UTF-8"));
                 
                 Document doc = builder.parse(input);
                 //Element root = doc.getDocumentElement();
                 doc.getDocumentElement().normalize();
                 
                 NodeList nList = doc.getElementsByTagName("document");
                 for (int i=0; i<nList.getLength(); i++){
                	 Node nNode = nList.item(i);
                	 Element eElement = (Element) nNode;
                	 String path = eElement.getElementsByTagName("path").item(0).getTextContent();//eElement.getAttribute("path").toString();
                	 String fldUuid = eElement.getElementsByTagName("uuid").item(0).getTextContent();
                	 System.out.println(eElement.getNodeName()+" "+path+" "+fldUuid);
                 }
                	 
            } else {
                System.err.println("Failed : HTTP error code : " + conn1.getResponseCode());
            }
            */
            
            //String u = "http://localhost:8080/OpenKM/services/rest/document/delete?docId=8eff5617-2243-4471-a72f-81a388c0f2d9";
            /*
            String u = "http://localhost:8080/OpenKM/services/rest/folder/delete?fldId=e01b10ea-64fd-45c4-a415-bd00ff8b35d0";
            URL url = new URL(u);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("DELETE");
			conn.setRequestProperty("Accept", "application/xml");
            //conn.getOutputStream();
			
			Authenticator.setDefault(new Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication("okmAdmin", "admin"
							.toCharArray());
				}
			});

			if (conn.getResponseCode() == 200) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						(conn.getInputStream())));
				String output;

				while ((output = br.readLine()) != null) {
					uuid = output;
				}
			} else {
				System.err.println("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}
			
			System.out.println("UUID is "+uuid);
			*/
            
            /*
            HttpClient httpclient = new DefaultHttpClient();
    		httpclient.getParams().setParameter(
    				CoreProtocolPNames.PROTOCOL_VERSION, HttpVersion.HTTP_1_1);
    		HttpPost httppost = new HttpPost(
    				"http://localhost:8180/OpenKM/services/rest/document/createSimple");
    		String encoding = DatatypeConverter.printBase64Binary("okmAdmin:admin"
    				.getBytes("UTF-8"));
    		httppost.setHeader("Authorization", "Basic " + encoding);
    		httppost.setHeader("Accept", "application/json");
    		MultipartEntity mpEntity = new MultipartEntity();
    		ContentBody docContent = new StringBody(content);
    		ContentBody docPath = new StringBody(path);
    		mpEntity.addPart("docPath", docPath);
    		mpEntity.addPart("content", docContent);
    		httppost.setEntity(mpEntity);
    		try {
    			HttpResponse response = httpclient.execute(httppost);
    			HttpEntity resEntity = response.getEntity();
    			if (resEntity != null) {
    				resEntity.consumeContent();
    			}
    			httpclient.getConnectionManager().shutdown();
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
			*/
			
            String[] cmd = {"/usr/bin/curl", "-u", "okmAdmin:admin", "-H", "Accept: application/json",
            		"-X", "POST", "-H", "Content-Type: application/json", "-d", "/okm:root/folder1",
            		"http://localhost:8080/OpenKM/services/rest/folder/createSimple"};
            		
            Process proc = Runtime.getRuntime().exec(cmd);
			proc.waitFor();
            
			String uuid = args[0];
			String command = ReconfigurableOpenKMClient.getDocumentChildrenCommand(uuid);
			cmd = command.split(",");
			proc = Runtime.getRuntime().exec(cmd);
			proc.waitFor();
			
			StringBuilder sbuilder = new StringBuilder();
			BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String line = "";
			while ((line = br.readLine()) != null) {
			    sbuilder.append(line+"\n");
			}
			br.close();
			
			DocumentBuilderFactory factory =
             DocumentBuilderFactory.newInstance();
             DocumentBuilder builder = factory.newDocumentBuilder();
             
             ByteArrayInputStream input =  new ByteArrayInputStream(
            		   sbuilder.toString().getBytes("UTF-8"));
             
             Document doc = builder.parse(input);
             //Element root = doc.getDocumentElement();
             doc.getDocumentElement().normalize();
             
             NodeList nList = doc.getElementsByTagName("document");
             for (int i=0; i<nList.getLength(); i++){
            	 Node nNode = nList.item(i);
            	 Element eElement = (Element) nNode;
            	 String path = eElement.getElementsByTagName("path").item(0).getTextContent();//eElement.getAttribute("path").toString();
            	 String fldUuid = eElement.getElementsByTagName("uuid").item(0).getTextContent();
            	 System.out.println(eElement.getNodeName()+" "+path+" "+fldUuid);
             }
			
            System.out.println("Nothing goes wrong!");
	}
}
