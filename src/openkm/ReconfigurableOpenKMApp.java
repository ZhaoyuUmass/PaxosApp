package openkm;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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
public class ReconfigurableOpenKMApp extends AbstractReconfigurablePaxosApp<String> 
implements Replicable, Reconfigurable, ClientMessenger{
	DocumentBuilder builder;
	
	/**
	 * 
	 */
	public ReconfigurableOpenKMApp(){
		try {
			builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
	}
	
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
		
		String op = request.getValue();
		String[] urls = op.split(ReconfigurableOpenKMClient.fdelimiter);
		
		/**
		 * Example for a delete operation:
		 * curl -u okmAdmin:admin -H "Accept:application/json" -X GET http://localhost:8080/OpenKM/services/rest/repository/getNodeUuid?nodePath=okm:root/test/1.txt
		 * curl -u okmAdmin:admin -H "Accept:application/json" -X DELETE http://localhost:8080/OpenKM/services/rest/document/delete?docId=
		 */
		String response = null;
		String last = "";
		for(int i=0; i<urls.length; i++){
			String url = urls[i] + last;
			try {
				response = executeCommand(url);
			} catch (Exception e) {
				e.printStackTrace();
			} 
			if (response != null){
				last = response;
			}else{
				break;
			}
		}
		this.sendResponse(request, response);
		System.out.println("Application execution time "+(System.currentTimeMillis() - start)+"ms");
		return true;
	}
	
	private String executeCommand(String operation) throws IOException, InterruptedException{
		
		String[] cmd = operation.split(ReconfigurableOpenKMClient.delimiter);
		System.out.println(operation);
		
		Process proc = Runtime.getRuntime().exec(cmd);
		proc.waitFor();
				
		StringBuilder builder = new StringBuilder();
		BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		String line = "";
		while ((line = br.readLine()) != null) {
		    builder.append(line+"\n");
		}
		br.close();

		return builder.toString();
	}
	
	private String processXMLAndGetDocList(String xml) throws SAXException, IOException, InterruptedException{
        String state = "";
        
        ByteArrayInputStream input =  new ByteArrayInputStream(
       		   xml.getBytes("UTF-8"));
        
        Document doc = builder.parse(input);
        doc.getDocumentElement().normalize();
        
        NodeList nList = doc.getElementsByTagName("document");
        for (int i=0; i<nList.getLength(); i++){
			Node nNode = nList.item(i);
			Element eElement = (Element) nNode;
			String path = eElement.getElementsByTagName("path").item(0).getTextContent();
			String uuid = eElement.getElementsByTagName("uuid").item(0).getTextContent();
			state = state + path+ReconfigurableOpenKMClient.delimiter+
					executeCommand(ReconfigurableOpenKMClient.getDocumentContentCommand(uuid)) + 
					ReconfigurableOpenKMClient.fdelimiter;
			System.out.println(eElement.getNodeName()+" "+path+" "+uuid);
        }
        
        state = state.substring(0,state.length()-1);
        
        return state;
	}
	
	@Override
	public boolean execute(Request request) {
		return execute(request, false);
	}
	
	@Override
	public String checkpoint(String name) {
		
		String state = null;
		String uuid = ReconfigurableOpenKMClient.getUuidCommand(name);
		try {
			// retrieve all documents' name and content
			String childrenXML = executeCommand(ReconfigurableOpenKMClient.getDocumentChildrenCommand(uuid));
			state = processXMLAndGetDocList(childrenXML);
			
			// delete the folder
			executeCommand(ReconfigurableOpenKMClient.getFolderDeleteCommand(uuid));
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
		return state;
	}

	@Override
	public boolean restore(String name, String state) {
		
		if(state != null){
			// re-create the folder
			String command = ReconfigurableOpenKMClient.createFolderCommand(name);
			try {
				executeCommand(command);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			String[] docs = state.split(ReconfigurableOpenKMClient.fdelimiter);
			for (String doc:docs){
				String[] params = doc.split(ReconfigurableOpenKMClient.delimiter);
				try {
					executeCommand(ReconfigurableOpenKMClient.createDocumentCommand(params[0], params[1]));
				} catch (Exception e) {
					e.printStackTrace();
				} 
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
	
	private void sendResponse(AppRequest request, String response) {
		// set to whatever response value is appropriate
		if(response != null){
			request.setResponse(response);
		}else{
			request.setResponse(ResponseCodes.ACK.toString());
		}
	}
}
