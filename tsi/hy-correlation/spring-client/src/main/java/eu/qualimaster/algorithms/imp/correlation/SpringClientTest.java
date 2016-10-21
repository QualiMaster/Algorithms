package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.data.inf.ISpringFinancialData;

/**
 * Created by Apostolos Nydriotis on 2015/10/29.
 */
public class SpringClientTest {

  public static void main(String[] args) {
    SpringClient client = new SpringClient();
    client.connect();
    Object symbolList = client.getSymbolList();
    Object springStream = client.getSpringStream();

    while (true) {
      ISpringFinancialData.ISpringFinancialDataSpringStreamOutput s = client.getSpringStream();
      if (s != null) {
        System.out.println(s.getSymbolTuple());
      }
    }
  }
}
