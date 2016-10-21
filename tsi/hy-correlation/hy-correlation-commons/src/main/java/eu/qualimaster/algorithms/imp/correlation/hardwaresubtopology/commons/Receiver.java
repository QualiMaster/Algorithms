package eu.qualimaster.algorithms.imp.correlation.hardwaresubtopology.commons;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import backtype.storm.utils.Utils;

/**
 * Created by Apostolos Nydriotis on 12/12/14.
 */
public class Receiver {

//  String ip = "147.27.39.12";
//  String ip = "localhost";
//  int port = 2401;
  String ip;
  int port;
  Socket sock = null;
  BufferedReader in = null;

  public Receiver() throws Exception {
    connect();
  }

  public Receiver(String ip, int port) throws Exception {
    this.ip = ip;
    this.port = port;
    connect();
  }

  public void connect() throws Exception {
    while (true) {
      sock = new Socket(ip, port);
      in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
      if (in == null) {
        Utils.sleep(100);
      } else {
        break;
      }
    }
  }

  public String receiveData() throws Exception {
    if (in.ready()) {  // This applies to read(), not readLine(). However it's probably ok for us.
                       //  If there is data, there will be a line.
      return in.readLine();
    }
    return null;
  }
}
