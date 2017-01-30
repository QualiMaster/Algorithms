package eu.qualimaster.algorithms.imp.correlation.spring;

import eu.qualimaster.dataManagement.DataManagementConfiguration;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.NoRouteToHostException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Vector;

/**
 * Class template to handle TCP/IP data connection
 */
public class DataConnector {

  public static String SERVER_IP = "84.200.210.254";// Quote Server IP
  public static int SERVER_Port = 9003; // Quote Server Port

  private boolean running = false;
  private Socket tcpSocket = null;
  private DataOutputStream out = null;
  private DataInputStream in = null;
  public static final int OK = 0;
  public static final int NO_INTERNET = 1;
  public static final int CONNECTION_ERROR = 2;
  private String userName = "";
  public boolean sending = false;
  public boolean loggedIn = false;
  public boolean gotQuote = false;
  public Vector nameList;
  public Vector list;
  public String data = new String();
  public String fullQuoteListResponce;
  public int soTimeout;

  private int loginStatus;
  public static final int LOGIN_SUCESS = 0;
  public static final int WAITING_FOR_LOGIN_RESPONSE = 1;
  public static final int ACCOUNT_IN_USE = 2;

  public int getLoginStatus() {
    return loginStatus;
  }

  public DataConnector() {
    soTimeout = 10 * 60 * 1000; // 10 min
    loginStatus = ACCOUNT_IN_USE; // default value so we start checking accounts
//    soTimeout = 5000;
  }

  /**
   * connect to server using socket connection
   *
   * @return Returns connection status, one of DataConnector.OK, DataConnector.NO_INTERNET,
   * DataConnector.CONNECTION_ERROR
   */
  public int connect() {
    if(!DataManagementConfiguration.getExternalServicePath().contains("/var/nfs")) {
      // Just a temporary "hack". Use the tunneling IP/port if the cluster does not have nfs.
      // TODO : Read configuration about Server IP and Server Port
      SERVER_IP = "godzilla.kbs.uni-hannover.de";// Tunneling
      SERVER_Port = 5566; // Tunneling
    }
    try {
      tcpSocket = new Socket(SERVER_IP, SERVER_Port);
      tcpSocket.setSoTimeout(soTimeout);
      out = new DataOutputStream(new BufferedOutputStream(tcpSocket.getOutputStream()));
      in = new DataInputStream(new BufferedInputStream(tcpSocket.getInputStream()));

      running = true;
      return OK;
    } catch (NoRouteToHostException e) {
      System.out.println(e);
      return NO_INTERNET;
    } catch (IOException e) {
      System.out.println(e);
      return CONNECTION_ERROR;

    }

  }

  public void execute() {

    if (running) {
      if (!sending && tcpSocket != null && in != null) {

        try {
          StringBuilder response = new StringBuilder();
          int c = 0;
          while (in != null && (c = in.read()) != -1) {
            response.append((char) c);
            if ((char) c == '!') {// All messages are separated by
              // '!'
              break;
            }

          }
//          if (afterTimeout) {
//            System.out.println("response = " + response);
//            System.out.println("c = " + c);
//            try {
//              Thread.sleep(100);
//            } catch (InterruptedException e) {
//              e.printStackTrace();
//            }
//          }
          resolveResponse(response.toString());
        } catch (SocketTimeoutException s) {
          // Save this identifier in order to detect timeout in SpringClient class
          data = "SocketTimedOut";
        } catch (Exception ex) {
          System.err
              .println("Server read error : " + ex.getMessage());
        }
      }

    }

  }

  /*
   * Login to server with username and password
   */
  public void login(String username, String pw) throws IOException {
    sendData(username + ",login," + pw + "!");
    loginStatus = WAITING_FOR_LOGIN_RESPONSE;
    this.userName = username;
  }

  /*
   * Logout from the server
   */
  public void logout() throws IOException {
    sendData(userName + ",logoff!");
  }

  /**
   * Request server to send symbols list
   */
  public void getSymbols() throws IOException {
    sendData(userName + ",quotelist!");
  }

  /**
   * Request server to start quotes for symbol
   *
   * @param Object symbol - The Object symbol to send to server
   */
  public void startQuote(Object symbol) throws IOException {
    String request = userName + ",orderquote,";

    sendData(request, false);
    sendSymbol(symbol + "");
    System.out.println("sent data : " + request + symbol + "!");

  }

  /**
   * Request server to to stop quotes for symbol
   *
   * @param Object symbol - The Object symbol to send to server
   */
  public void stopQuote(Object symbol) throws IOException {
    String request = userName + ",cancelquote,";
    sendData(request, false);
    sendSymbol(symbol + "");
    System.out.println("sent data : " + request + symbol + "!");
  }

  /**
   * Request server to to start depth for symbol
   *
   * @param Object symbol - The Object symbol to send to server
   */
  public void startDepth(Object symbol) throws IOException {
    String request = userName + ",orderdepth,";
    sendData(request, false);
    sendSymbol(symbol + "");
    System.out.println("sent data : " + request + symbol + "!");
  }

  /**
   * Request server to to stop depth for symbol
   *
   * @param Object symbol - The Object symbol to send to server
   */
  public void stopDepth(Object symbol) throws IOException {
    String request = userName + ",canceldepth,";
    sendData(request, false);
    sendSymbol(symbol + "");
    System.out.println("sent data : " + request + symbol + "!");
  }

  /**
   * private method to only send symbol data to server (this method invoke from an other method
   * inside this class)
   *
   * @param String symbol - The String symbol to send to server
   */
  private void sendSymbol(String symbol) throws IOException {

    if (out != null && symbol != null && !symbol.trim().isEmpty()) {

      symbol = symbol.trim();
      String ar[] = symbol.split("�");
      out.writeBytes(ar[0]);
      out.write(183);
      out.writeBytes(ar[1]);
      out.write(183);
      out.writeBytes(ar[2]);
      out.writeBytes("!");
      out.flush();

    }
  }

  /**
   * Send data to server
   *
   * @param String line - The String line to send to server
   */
  public void sendData(String line) throws IOException {
    sendData(line, true);
  }

  /**
   * Send data to server and prints sent data in the standard output (command line)
   *
   * @param line  - The String line to send to server
   * @param print - Whether it should print in the command line, or not
   */
  public void sendData(String line, boolean print) throws IOException {
    // All messages are separated by '!'

    if (out != null) {
      out.writeBytes(line);
      out.flush();
      if (print) {
        System.out.println("sent data : " + line);
      }
    }
  }

  /**
   * Decide the actions for the received response from the server
   *
   * @param String response- The response line received from server
   */
  private void resolveResponse(String response) {
    if (response == null) {
      return;
    }

    if (response.replace("!", "").trim().isEmpty()) {
      return;
    }
    response = response.trim();
    //System.out.println(response.toString());
    if (response.toUpperCase().startsWith("LOGIN,")) {
      loggedIn = true;
      System.out.println(response.toString());
      if(response.toLowerCase().contains("already logged in")) {
        loginStatus = ACCOUNT_IN_USE;
      } else {
        loginStatus = LOGIN_SUCESS;
      }
    } else if (response.toUpperCase().startsWith("ORDERQUOTE,")) {
      System.out.println(response.toString());
    } else if (response.toUpperCase().startsWith("QUOTELIST,")) {

      response = response.substring(10);
      fullQuoteListResponce = response;
      String lines[] = response.split("\n");
      list = new Vector();
      nameList = new Vector();
      for (int i = 0; i < lines.length; i++) {

        if (lines[i] != null) {
          String line = lines[i];
          String data[] = line.split("\\|");
          if (data.length > 3) {
            nameList.add((data[0] + "�" + data[1] + "�" + data[3]));
            list.add(data[13]);
          }
        }
      }
    } else if (response.toUpperCase().startsWith("HEARTBEAT,")) {
      try {
        sendData(userName + ",heartbeatanswer,!");
      } catch (IOException ex) {
        System.err.println(ex.getMessage());
      }
    } else if (response.toUpperCase().startsWith("QUOTE,")) {
      gotQuote = true;
      String ar[] = response.split(",");
      if (ar.length > 7) {
        String line = ar[6] + "," + ar[7] + "," + ar[4] + "," + ar[5];
        //System.out.println(ar[1] + " " + line);
        data = ar[1] + "," + line;
      }
    } else if (response.toUpperCase().startsWith("DEPTH,")) {

      String line = response.endsWith("!") ? response.substring(0,
                                                                response.length() - 1) : response;
      System.out.println("depth: " + line);

    } else if (response.toUpperCase().startsWith("ERROR,")) {
      System.out.println("Error: " + response.toString());
    } else {
      System.out.println("got data  : " + response.toString());
    }

  }

  public String getData() {
    return data;
  }

  public boolean isLoggedIn() {
    return loggedIn;
  }

  /**
   * Stop the server TCP IP connection
   */
  public void stopRunning() {
    running = false;
//		loggedIn = false;
//		try {
//			stop();
//		} catch (Exception e) {
//		}
    try {
      in.close();
      in = null;
    } catch (Exception e) {
    }
    try {
      out.close();
      out = null;
    } catch (Exception e) {
    }
    try {
      tcpSocket.shutdownInput();
    } catch (Exception e) {
    }
    try {
      tcpSocket.shutdownOutput();
    } catch (Exception e) {
    }
    try {
      tcpSocket.close();
    } catch (Exception e) {
    }
  }

}
