package com.ibm.odm;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import com.ibm.json.java.JSONObject;

/**
 * The purpose of this class is to replicate a RuleApp and its supporting XOM
 * libraries/resources from a source RES to a destination RES. The replicate
 * method selects the latest version of the RuleApp from the source RES. It is
 * applicable to version 8.8 and before, where the XOM is not packaged with the
 * RuleApp.
 * 
 * See also: 
 * - http://www.ibm.com/support/knowledgecenter/SS7J8H/com.ibm.odm.cloud.deploy/topics/tsk_hybrid_cloud_curl.html
 * - Product sample: <odminstall>/executionserver/samples/restapi
 * 
 * @author pberland@us.ibm.com
 *
 */
public class RESPromoter {

	// Source RES
	protected HttpClient srcClient;
	protected String srcRestUrl;

	// Destination RES
	protected HttpClient dstClient;
	protected String dstRestUrl;

	private static Logger logger = Logger.getLogger(RESPromoter.class.getName());

	public RESPromoter(ServerInfo srcServerInfo, ServerInfo dstServerInfo) {
		this.srcClient = createClient(srcServerInfo);
		this.srcRestUrl = srcServerInfo.getRestUrl();

		this.dstClient = createClient(dstServerInfo);
		this.dstRestUrl = dstServerInfo.getRestUrl();
	}

	/**
	 * Creates an HTTP client for the given server and credentials.
	 * 
	 * @param serverInfo @return @throws
	 */
	protected HttpClient createClient(ServerInfo serverInfo) {
		HttpClient client = new HttpClient();
		client.getParams().setAuthenticationPreemptive(true);

		Credentials credentials = new UsernamePasswordCredentials(serverInfo.getUsername(), serverInfo.getPassword());
		int port = (serverInfo.getServerPort() == null) ? AuthScope.ANY_PORT
				: Integer.valueOf(serverInfo.getServerPort());
		AuthScope scope = new AuthScope(serverInfo.getServerName(), port);
		client.getState().setCredentials(scope, credentials);
		logger.info("Set credentials for " + serverInfo.getServerName());
		return client;
	}

	protected boolean hasFailed(int statusCode) {
		return (statusCode != HttpStatus.SC_OK) && (statusCode != HttpStatus.SC_NO_CONTENT)
				&& (statusCode != HttpStatus.SC_CREATED);
	}

	/**
	 * Calls GET method on the given url/resource and returns the result in a JSON
	 * object or null when there is no result.
	 * 
	 * @param url
	 * @param resource
	 * @return
	 * @throws Exception
	 */
	protected JSONObject execGetMethod(HttpClient client, String url, String resource) throws Exception {
		String jsonUrl = url + resource + "?accept=json";
		HttpMethod method = new GetMethod(jsonUrl);
		try {
			if (hasFailed(client.executeMethod(method))) {
				throw new Exception("Method execution failed: " + method.getStatusLine());
			}
			InputStream stream = method.getResponseBodyAsStream();
			return (stream == null) ? null : JSONObject.parse(stream);
		} finally {
			method.releaseConnection();
		}
	}

	/**
	 * Calls GET method on the given url/resource and returns the resulting archive
	 * in file.
	 * 
	 * @param url
	 * @param resource
	 * @param archiveFilename
	 * @throws Exception
	 */
	protected void execGetMethod(HttpClient client, String url, String resource, String archiveFilename)
			throws Exception {
		String fullUrl = url + resource;
		HttpMethod method = new GetMethod(fullUrl);
		try {
			if (hasFailed(client.executeMethod(method))) {
				throw new Exception("Method execution failed: " + method.getStatusLine());
			}
			saveArchive(method.getResponseBodyAsStream(), archiveFilename);
		} finally {
			method.releaseConnection();
		}
	}

	/**
	 * Calls POST method on the given url/resource and JSON body.
	 * 
	 * @param url
	 * @param resource
	 * @param body
	 * @throws Exception
	 */
	protected void execPostMethod(HttpClient client, String url, String resource, String body, String type)
			throws Exception {
		String fullUrl = url + resource;
		PostMethod method = new PostMethod(fullUrl);
		System.out.println(body.toString());
		System.out.println(fullUrl);
		RequestEntity entity = new StringRequestEntity(body, type, "UTF8");
		// new StringRequestEntity(body.toString(), "application/json", "UTF-8");
		method.setRequestEntity(entity);
		try {
			if (hasFailed(client.executeMethod(method))) {
				throw new Exception("Method execution failed: " + method.getStatusLine());
			}
		} finally {
			method.releaseConnection();
		}
	}

	/**
	 * Calls POST method on the given url/resource and string byte array.
	 * 
	 * @param url
	 * @param resource
	 * @param byteBody
	 * @throws Exception
	 * @throws IOException
	 * @throws Exception
	 */
	protected void execPostMethod(HttpClient client, String url, String resource, byte[] byteBody) throws Exception {
		String fullUrl = url + resource;
		PostMethod method = new PostMethod(fullUrl);
		RequestEntity entity = new ByteArrayRequestEntity(byteBody, "application/octet-stream");
		method.setRequestEntity(entity);
		try {
			if (hasFailed(client.executeMethod(method))) {
				throw new Exception("Method execution failed: " + method.getStatusLine());
			}
		} finally {
			method.releaseConnection();
		}
	}

	protected void saveArchive(InputStream stream, String filename) throws Exception {
		try {
			int count;
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			while ((count = stream.read(buffer)) != -1) {
				bos.write(buffer, 0, count);
			}
			FileOutputStream fos = new FileOutputStream(filename);
			bos.writeTo(fos);
			fos.close();
		} catch (Exception e) {
			throw new Exception("Failed to save archive to " + filename);
		}
	}

	protected byte[] getArchive(String filename) throws Exception {
		try {
			File file = new File(filename);
			logger.info("File path: " + file.getAbsolutePath());
			InputStream stream = new FileInputStream(filename);
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int count;
			while ((count = stream.read(buffer)) != -1) {
				bos.write(buffer, 0, count);
			}
			stream.close();
			logger.info("Read " + bos.size() + " bytes from " + filename);
			// return DatatypeConverter.printBase64Binary(bos.toByteArray());
			return bos.toByteArray();
		} catch (Exception e) {
			throw new Exception("Failed to read archive from " + filename);
		}
	}

	protected void replicateRuleapp(JSONObject srcRuleappDef, String ruleappName, boolean execute) throws Exception {
		String resource = null;

		String id = (String) srcRuleappDef.get("id");
		String ruleappVersion = id.substring(id.indexOf("/") + 1);
		logger.info("Found version " + ruleappVersion + " for " + ruleappName + " on source RES");
		//
		// Verify that the RuleApp is not already deployed on the destination RES.
		//
		resource = "/ruleapps/" + ruleappName + "/" + ruleappVersion;
		JSONObject ruleappDef = execGetMethod(dstClient, dstRestUrl, resource);
		if (ruleappDef != null) {
			logger.severe("RuleApp " + ruleappName + " is already deployed on destination RES");
			return;
		}
		//
		// Get the RuleApp archive from the source RES.
		//
		resource = resource + "/archive";
		String archiveName = "./data/" + ruleappName + "_ruleapp.jar";
		execGetMethod(srcClient, srcRestUrl, resource, archiveName);
		logger.info("Saved source RES archive to file " + archiveName);
		//
		// Deploy the RuleApp archive to the destination RES.
		//
		resource = "/ruleapps";
		if (execute) {
			execPostMethod(dstClient, dstRestUrl, resource, getArchive(archiveName));
		}
		logger.info("Deployed RuleApp archive " + archiveName + " to " + dstRestUrl);
	}

	@SuppressWarnings("unchecked")
	protected void replicateXOMs(JSONObject srcRuleappDef, String ruleappName, boolean execute) throws Exception {
		// Get all the managed XOM information.
		//
		List<String> xomUris = new ArrayList<String>();
		List<JSONObject> srcRulesetDefs = (List<JSONObject>) srcRuleappDef.get("rulesets");
		for (JSONObject rulesetDef : srcRulesetDefs) {
			List<JSONObject> properties = (List<JSONObject>) rulesetDef.get("properties");
			for (JSONObject property : properties) {
				if (((String) property.get("id")).equals("ruleset.managedxom.uris")) {
					String xomUri = (String) property.get("value");
					if (!xomUris.contains(xomUri)) {
						xomUris.add(xomUri);
						logger.info("Found managed XOM entry used by source RuleApp: " + xomUri);
					}
				}
			}
		}
		//
		// Process managed XOM entries, where the uri can either point to a library
		// (reslib) or a XOM (resuri).
		//
		String resource;
		for (String uri : xomUris) {
			if (uri.contains("reslib")) {
				resource = "/libraries/" + uri.substring(uri.indexOf("//") + 2);
				//
				// Verify library is not already present on the destination RES.
				//
				JSONObject libDef = execGetMethod(dstClient, dstRestUrl, resource);
				if (libDef != null) {
					logger.info("Library " + uri + " is already deployed on the destination RES");
					continue;
				}
				libDef = execGetMethod(srcClient, srcRestUrl, resource);
				List<String> contentDefs = (List<String>) libDef.get("content");
				String contentBody = "";
				for (int i = 0; i < contentDefs.size(); i++) {
					contentBody += contentDefs.get(i);
					if (i != contentDefs.size() - 1) {
						contentBody += ", ";
					}
				}
				System.out.println("URIS " + contentBody);
				if (execute) {
					execPostMethod(dstClient, dstRestUrl, resource, contentBody, "text/plain");
				}
				logger.info("Deploy library definition " + resource + " to " + dstRestUrl);
				//
				// Deploy content of library.
				//
				for (String xomUri : contentDefs) {
					replicateXOM(xomUri, execute);
				}
			} else {
				replicateXOM(uri, execute);
			}
		}
	}

	protected void replicateXOM(String xomUri, boolean execute) throws Exception {
		// Verify XOM is not already present on the destination RES.
		//
		String xomNameAndVersion = xomUri.substring(xomUri.indexOf("//") + 2);
		String xomName = xomNameAndVersion.substring(0, xomNameAndVersion.indexOf("/"));
		String resource = "/xoms/" + xomNameAndVersion;
		JSONObject libDef = execGetMethod(dstClient, dstRestUrl, resource);
		if (libDef != null) {
			logger.info("XOM " + xomUri + " is already deployed on the destination RES");
		}
		//
		// Get the XOM archive from the source RES.
		//
		resource = resource + "/bytecode";
		String archiveName = "./data/" + xomName;
		execGetMethod(srcClient, srcRestUrl, resource, archiveName);
		logger.info("Saved source RES xom to file " + archiveName);
		//
		// Deploy the XOM archive to the destination RES.
		//
		resource = "/xoms/" + xomName;
		if (execute) {
			execPostMethod(dstClient, dstRestUrl, resource, getArchive(archiveName));
		}
		logger.info("Deployed XOM archive " + archiveName + " to " + dstRestUrl);
	}

	/**
	 * Replicates the latest version of the named RuleApp from the source server to
	 * the destination server. If the execute flag is false, the deployment to the
	 * destination server is only simulated.
	 * 
	 * @param ruleappName
	 * @param execute
	 * @throws Exception
	 */
	public void replicate(String ruleappName, boolean execute) throws Exception {
		// Find the highest version of the RuleApp from the source RES.
		//
		String resource = "/ruleapps/" + ruleappName + "/highest";
		JSONObject srcRuleappDef = execGetMethod(srcClient, srcRestUrl, resource);
		if (srcRuleappDef == null) {
			logger.severe("RuleApp " + ruleappName + " is not deployed on source RES");
			return;
		}
		replicateRuleapp(srcRuleappDef, ruleappName, execute);
		replicateXOMs(srcRuleappDef, ruleappName, execute);
	}

	/**
	 * Sample execution.
	 * @param args
	 */
	public static void main(String[] args) {
		// Define the source and destination RES.
		ServerInfo srcServerInfo = new ServerInfo("localhost", "9081", "resAdmin", "resAdmin");
		ServerInfo dstServerInfo = new ServerInfo("localhost", "9080", "rtsAdmin", "rtsAdmin");

		RESPromoter client = new RESPromoter(srcServerInfo, dstServerInfo);
		try {
			client.replicate("promote", true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
