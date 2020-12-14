package com.ibm.odm;

/**
 * Utility class to capture the RES URL and credentials.
 * 
 * @author pberland@us.ibm.com
 *
 */
public class ServerInfo {
	private String serverName;
	private String serverPort;
	private String username;
	private String password;

	public ServerInfo(String serverName, String serverPort, String username, String password) {
		super();
		this.serverName = serverName;
		this.serverPort = serverPort;
		this.username = username;
		this.password = password;
	}

	/**
	 * Creates the RES URL to use for REST methods calls.
	 * 
	 * @param serverInfo
	 * @return
	 */
	public String getRestUrl() {
		String restURL = "http://" + serverName;
		if (serverPort != null) {
			restURL += ":" + serverPort;
		}
		return restURL + "/res/apiauth/v1";
	}

	public String getServerName() {
		return serverName;
	}

	public String getServerPort() {
		return serverPort;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

}
