package eu.qualimaster.algorithms.imp.correlation.hardwaresubtopology.commons;

import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by Apostolos Nydriotis on 12/12/14.
 */
public class Transmitter {

  String ip = "147.27.39.12";
  //String ip = "localhost";
  int port = 2400;

  Socket sock = null;
  PrintWriter out = null;

  public Transmitter() throws Exception {
    connect();
  }

  public Transmitter(String ip, int port) throws Exception {
    this.ip = ip;
    this.port = port;
    connect();
  }

  public void connect() throws Exception {
    sock = new Socket(ip, port);
    out = new PrintWriter(sock.getOutputStream(), true);
  }

  public void sendData(String data) throws Exception {
    send("d " + data);
  }

  public void sendConfiguration(String configuration) throws Exception {
    send("c " + configuration);
  }

  public void sendResultRequest() throws Exception {
    send("r");
  }

  private void send(String data) throws Exception {
    out.println(data);
    out.flush();
  }
}
