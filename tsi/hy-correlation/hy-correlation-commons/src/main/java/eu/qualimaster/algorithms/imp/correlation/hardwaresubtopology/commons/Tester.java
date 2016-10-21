package eu.qualimaster.algorithms.imp.correlation.hardwaresubtopology.commons;

import java.io.IOException;

/**
 * Created by Apostolos Nydriotis on 12/12/14.
 */
public class Tester {

  public static void main(String[] args) {
    try {
      Transmitter transmitter = new Transmitter("localhost", 2400);
      Receiver receiver = new Receiver("localhost", 2401);

      int t = 10;

      while (--t >= 0) {
        transmitter.sendData("dato " + t);
        System.out.println(receiver.receiveData());
      }

    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
