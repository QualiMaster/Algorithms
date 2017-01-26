package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.algorithms.imp.correlation.spring.DataConnector;
import eu.qualimaster.algorithms.imp.correlation.spring.DataOutputController;
import eu.qualimaster.data.imp.SpringFinancialData;
import eu.qualimaster.data.inf.ISpringFinancialData;
import eu.qualimaster.dataManagement.DataManagementConfiguration;
import eu.qualimaster.dataManagement.accounts.PasswordStore;
import eu.qualimaster.dataManagement.sources.IDataSourceListener;
import eu.qualimaster.dataManagement.sources.IHistoricalDataProvider;
import eu.qualimaster.dataManagement.sources.SpringHistoricalDataProvider;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.pipeline.DefaultModeException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Apostolos Nydriotis on 12/10/14.
 */
public class SpringClient implements ISpringFinancialData {

  static {
    DataManagementConfiguration.configure(new File("/var/nfs/qm/qm.infrastructure.cfg"));
  }

  DataConnector connector;
  DataOutputController dataOutputController;
  Logger logger = Logger.getLogger(SpringClient.class);
  List<String> allSymbolsList;
  long lastConfigurationEmittion;
  private boolean saveLocalData = false;
  private Map<String, String> idsToNamesMap;
  private IDataSourceListener mappingChangedListener;
  private boolean connected;

  public SpringClient() {
    connected = false;
  }

  @Override public ISpringFinancialDataSpringStreamOutput getSpringStream() throws DefaultModeException {
    if (!connected) {
      return null;
    }

    connector.execute();
    String tmpData = connector.getData();
    if (tmpData != null && !tmpData.equals("")) {
      if (tmpData.equals("SocketTimedOut")) { // Means socket timed out. Re-connect
        System.out.println("\nTimeout! Login again.");
        try {
          connector.logout();
        } catch (IOException e) {
          e.printStackTrace();
        }
        connector.execute();

        allSymbolsList = new ArrayList<>();
        idsToNamesMap = new HashMap<>();
        connect();
        return null;
      }
      if (saveLocalData) {
        try {
          dataOutputController.saveQuoteData(tmpData);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          logger.error("Error : " + e.getMessage());
          throw new DefaultModeException("Error : " + e.getMessage());
        }
      }

      SpringFinancialData.SpringFinancialDataSpringStreamOutput symbolTuple =
        new SpringFinancialData.SpringFinancialDataSpringStreamOutput();
      symbolTuple.setSymbolTuple(tmpData);
      return symbolTuple;
    }
    return null;
  }

  @Override public String getAggregationKey(ISpringFinancialDataSpringStreamOutput tuple) {
    String result;
    String data = tuple.getSymbolTuple();
    int pos = data.indexOf(",");
    if (pos > 0) {
      result = data.substring(0, pos);
    } else {
      result = "";
    }
    return result;
  }

  @Override public ISpringFinancialDataSymbolListOutput getSymbolList() {
    if (!connected) {
      return null;
    }

    long now = System.currentTimeMillis();

    if (now - lastConfigurationEmittion >= 10000 || lastConfigurationEmittion == 0) {  // resend every 10 secs
      lastConfigurationEmittion = now;
      SpringFinancialData.SpringFinancialDataSymbolListOutput allSymbols =
        new SpringFinancialData.SpringFinancialDataSymbolListOutput();
      allSymbols.setAllSymbols(allSymbolsList);
      return allSymbols;
    }
    return null;
  }

  @Override public String getAggregationKey(ISpringFinancialDataSymbolListOutput tuple) {
    return null;
  }

  private void loginAction(String user) throws Exception {
    connector = new DataConnector();

    int i = 0;
    String userAccount;
    while (connector.getLoginStatus() == DataConnector.ACCOUNT_IN_USE) {
      userAccount = "tsi" + ++i;
      logger.info("Choosing account " + userAccount);
      PasswordStore.PasswordEntry entry = PasswordStore.getEntry(userAccount);
      String username = entry.getUserName();
      String password = entry.getPassword();

      int result = connector.connect();
      while (result != DataConnector.OK) {
        switch (result) {
          case DataConnector.CONNECTION_ERROR:
            logger.error("SERVER: Connection Error");
            //          throw new Exception("SERVER: Connection Error");
          case DataConnector.NO_INTERNET:
            logger.error("SERVER: Connection Error, Check your internet connection");
            //          throw new Exception("SERVER: Connection Error, Check your internet connection");
        }
        Thread.sleep(10000);
        result = connector.connect();
      }
      logger.info("SERVER: Connection success");
      try {
        connector.login(username, password);
        while (connector.getLoginStatus() == DataConnector.WAITING_FOR_LOGIN_RESPONSE) {
          connector.execute();
        }
      } catch (IOException ex) {
        logger.error("SERVER: Login Error : " + ex.getMessage());
        throw new Exception("SERVER: Login Error : " + ex.getMessage());
      }
    }
  }

  private void getSymbolsAction() throws Exception {
    try {
      logger.info("Please wait until symbols loaded...");
      connector.getSymbols();
    } catch (IOException ex) {
      logger.error("SERVER: Get Symbols Error, " + ex.getMessage());
      throw new Exception("SERVER: Get Symbols Error : " + ex.getMessage());
    }
  }

  public void startQuote(String marketPlayerId) throws Exception {
    if (connector.isLoggedIn()) {
      connector.sending = true;
      try {
        connector.startQuote(idsToNamesMap.get(marketPlayerId));
        Thread.sleep(1);
      } catch (Exception ex) {
        throw new Exception(
          "SERVER: Start Quote [" + marketPlayerId + "] = " + idsToNamesMap.get(marketPlayerId) + " Error, "
            + ex.getMessage());
      }
      connector.sending = false;
    }
  }

  public void stopQuote(String marketPlayerId) throws Exception {
    if (connector.isLoggedIn()) {
      connector.sending = true;
      try {
        connector.stopQuote(idsToNamesMap.get(marketPlayerId));
        Thread.sleep(1);
      } catch (Exception ex) {
        throw new Exception(
          "SERVER: Stop Quote [" + marketPlayerId + "] = " + idsToNamesMap.get(marketPlayerId) + " Error, "
            + ex.getMessage());
      }
      connector.sending = false;
    }
  }

  private void startQuoteAllAction() throws Exception {
    if (connector.isLoggedIn()) {
      int count = connector.list.size();
      System.err.println(count);
      connector.sending = true;

      for (int i = 0; i < count; i++) {
        String value = connector.nameList.get(i) + "";
        try {
          connector.startQuote(value);
          Thread.sleep(1);
        } catch (Exception ex) {
          throw new Exception("SERVER: Start Quote " + value + " Error, " + ex.getMessage());
        }
      }
      connector.sending = false;
    }
  }

  /**
   * Fetches the symbol list (no subscription) and logs in
   */
  public void init(String username) {

    logger.info("Connecting...");

    allSymbolsList = new ArrayList<>();
    idsToNamesMap = new HashMap<>();
    lastConfigurationEmittion = 0;

    connected = true;

    dataOutputController.init();
    try {
      loginAction(username);
    } catch (Exception e) {
      throw new DefaultModeException(e.getMessage());
    }
    while (!connector.isLoggedIn()) {
      connector.execute();
    }

    try {
      getSymbolsAction();
    } catch (Exception e) {
      throw new DefaultModeException(e.getMessage());
    }
    while (connector.list == null) {
      connector.execute();
    }

    if (saveLocalData) {
      for (int i = 0; i < connector.list.size(); i++) {
        String line = connector.list.get(i).toString();
        try {
          dataOutputController.saveQuoteData(line);
        } catch (Exception e) {
          logger.error("Error : " + e.getMessage());
        }
      }
      try {
        dataOutputController.saveQuoteData("--------------------------------");
      } catch (Exception e) {
        logger.error("Error : " + e.getMessage());
      }
    }

    for (int i = 0; i < connector.nameList.size(); i++) {
      String value = connector.nameList.get(i) + "";
      String key = connector.list.get(i) + "";
      allSymbolsList.add(key);
      idsToNamesMap.put(key, value);
    }
    if (mappingChangedListener != null) {
      mappingChangedListener.notifyIdsNamesMapChanged();
    }
  }

  @Override public void connect() {
    init("springClient");

    try {
      startQuoteAllAction();
    } catch (Exception e) {
      throw new DefaultModeException(e.getMessage());
    }
    while (!connector.gotQuote) {
      connector.execute();
    }
  }

  @Override public void disconnect() {
    try {
      connector.logout();
    } catch (IOException e) {
      logger.warn("Tried to logout from the API, but something went wrong with the connection.");
      // Session will timeout an un-subscribe us anyway.
    }
    allSymbolsList = null;
    idsToNamesMap = null;
    connected = false;
  }

  @Override public IStorageStrategyDescriptor getStrategy() {
    return null;
  }

  @Override public void setStrategy(IStorageStrategyDescriptor iStorageStrategyDescriptor) {
  }

  @Override public Double getMeasurement(IObservable iObservable) {
    return null;
  }

  @Override public IHistoricalDataProvider getHistoricalDataProvider() {
    return new SpringHistoricalDataProvider();
  }

  @Override public Map<String, String> getIdsNamesMap() {
    return this.idsToNamesMap;
  }

  @Override public void setDataSourceListener(IDataSourceListener listener) {
    this.mappingChangedListener = listener;
    listener.notifyIdsNamesMapChanged();
  }

  // put this code every time the ids-names mapping is changed
  //  if (null != mappingChangedListener) {
  //	  mappingChangedListener.notifyIdsNamesMapChanged();
  //	}

}
