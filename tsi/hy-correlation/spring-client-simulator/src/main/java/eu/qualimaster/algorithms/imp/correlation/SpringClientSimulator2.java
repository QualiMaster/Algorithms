package eu.qualimaster.algorithms.imp.correlation;

import eu.qualimaster.data.imp.SimulatedFinancialData;
import eu.qualimaster.data.inf.ISimulatedFinancialData;
import eu.qualimaster.dataManagement.DataManagementConfiguration;
import eu.qualimaster.dataManagement.sources.IDataSourceListener;
import eu.qualimaster.dataManagement.sources.IHistoricalDataProvider;
import eu.qualimaster.dataManagement.sources.SpringHistoricalDataProvider;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.pipeline.DefaultModeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Nikolaos Pavlakis on 1/13/15.
 */

public class SpringClientSimulator2 implements ISimulatedFinancialData {

  static {
    DataManagementConfiguration.configure(new File("/var/nfs/qm/qm.infrastructure.cfg"));
  }

  List<String> allSymbolsList;
  Logger logger = Logger.getLogger(SpringClientSimulator2.class);
  // For HDFS
  Configuration hdfsConfig;
  FileSystem fs;
  // TODO: Call mappingChangedListener.notifyIdsNamesMapChanged(); when the mapping is changed
  IDataSourceListener mappingChangedListener;
  private boolean useHdfs = true;
  private String hdfsUrl = "";
  // /For HDFS
  private String pathToSymbolList, pathToData;
  private File fileForList, fileForData;
  private BufferedReader brForList, brForData;
  private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss");
  private DateTimeFormatter dtf = DateTimeFormat.forPattern("MM/dd/yyyy' 'HH:mm:ss");
  private long offsetInMillis; // Offset in milliseconds between first timestamp and now
  private long thisTimeStampNow;
  private long prevTimeStampNow;
  private double speedFactor;
  private long lastConfigurationEmittion;
  private boolean shouldReadNextLine;
  private String lineRead;
  // Holds the line that was read from the file, so that we can emit it once the time passes
  private long startSleeping;
  private Map<String, String> idsToNamesMap;
  // Throughput measurement
  private long financialMonitoringTimestamp;
  private long finacialThroughput;
  private boolean isConnected;
  // ---------------------
  private int measurementDuration;  // seconds

  public SpringClientSimulator2() {
    String symbolListFileName = "Symbollist.txt";
    String dataFilename = "data.txt";
    useHdfs = DataManagementConfiguration.useSimulationHdfs();

    String pathPrefix = "";

    if (useHdfs) {
      logger.info("Using hdfs for simulation data");
      hdfsUrl = DataManagementConfiguration.getHdfsUrl();
      if (hdfsUrl.equals("")) {
        hdfsUrl = "hdfs://snf-618466.vm.okeanos.grnet.gr:8020";
        logger.warn("hdfs.url is empty! Using default: " + hdfsUrl);
      } else {
        logger.info("Configured hdfs.url: " + hdfsUrl);
      }
      pathPrefix = DataManagementConfiguration.getHdfsPath();
      if (pathPrefix.equals("")) {
        pathPrefix = "/user/storm/";
        logger.warn("hdfs.path is empty! Using default: " + pathPrefix);
      } else {
        logger.info("Configured hdfs.path: " + pathPrefix);
      }
    } else {
      logger.info("Using local FS for simulation data");
      pathPrefix = DataManagementConfiguration.getSimulationLocalPath();
    }
    pathToSymbolList = pathPrefix + "/" + symbolListFileName;
    pathToData = pathPrefix + "/" + dataFilename;
    logger.info("Path to Symbollist.txt: " + pathToSymbolList);
    logger.info("Path to data.txt: " + pathToData);
    //logger.info("Logs of aggregation keys are active");
    logger.info("Using · as separator for term names");
    logger.info("Also trying ï¿½");
  }

  private String newlineWithDateToNow(String line) {
    //split data
    String ar[] = line.split(",");
    String dateStr = ar[1] + " " + ar[2];
    //end split
    String newline = null;
    try {
      DateTime symbolTimeStamp = new DateTime(sdf.parse(dateStr)
                                                 .getTime());
      DateTime symbolTimeStampNow = symbolTimeStamp.plus(offsetInMillis);
      thisTimeStampNow = symbolTimeStampNow.getMillis();
      //            System.out.println(
      //                "Original " + symbolTimeStamp + " New " + symbolTimeStampNow + " offset "
      //                    + offsetInMillis);
      //rejoin data
      String newDate[] = symbolTimeStampNow.toString(dtf)
                                           .split(" ");
      newline = ar[0] + "," + newDate[0] + "," + newDate[1] + "," + ar[3] + "," + ar[4];
      //end rejoin
    } catch (ParseException e) {
      logger.error("Simulator Error : " + e.getMessage());
    }
    return newline;
  }

  private void updateThisTimeStampNow(String line) {
    //split data
    String ar[] = line.split(",");
    String dateStr = ar[1] + " " + ar[2];
    //end split
    try {
      DateTime symbolTimeStamp = new DateTime(sdf.parse(dateStr)
                                                 .getTime());
      DateTime symbolTimeStampNow = symbolTimeStamp.plus(offsetInMillis);
      thisTimeStampNow = symbolTimeStampNow.getMillis();
    } catch (ParseException e) {
      logger.error("Simulator Error : " + e.getMessage());
    }
  }

  @Override public ISimulatedFinancialDataSpringStreamOutput getSpringStream() throws DefaultModeException {
    if (!isConnected) {  // contract: return null if not connected
      return null;
    }

    if (prevTimeStampNow == 0) {
      prevTimeStampNow = thisTimeStampNow;
    }
    if (prevTimeStampNow != thisTimeStampNow) {
      if (startSleeping == 0) {
        startSleeping = System.currentTimeMillis();
      }
      try {
        double diff = ((double) (thisTimeStampNow - prevTimeStampNow)) / speedFactor;
        if (startSleeping + diff > System.currentTimeMillis()) {
          Thread.sleep(1);
          return null;
        }
      } catch (InterruptedException e) {
        logger.error("Simulator Error : " + e.getMessage());
      }
      startSleeping = 0;
      prevTimeStampNow = thisTimeStampNow;
    }
    ISimulatedFinancialDataSpringStreamOutput symbolTuple =
      new SimulatedFinancialData.SimulatedFinancialDataSpringStreamOutput();
    symbolTuple.setSymbolTuple(lineRead);

    try {
      if ((lineRead = brForData.readLine()) != null) {
        if (lineRead.startsWith(" ") || lineRead.equals("")) {
          return null;
        }
        updateThisTimeStampNow(lineRead);
      }
    } catch (IOException e) {
      logger.error("Simulator Error : " + e.getMessage());
      throw new DefaultModeException("Simulator Error : " + e.getMessage());
    }
    return symbolTuple;
  }

  @Override public String getAggregationKey(ISimulatedFinancialDataSpringStreamOutput tuple) {
    String result;
    String data = tuple.getSymbolTuple();
    int pos = data.indexOf(",");
    if (pos > 0) {
      result = data.substring(0, pos);
    } else {
      result = "";
    }
    //logger.info("Aggregation key: " + result);
    return result;
  }

  @Override public void setParameterSpeedFactor(double v) {
    setSpeed(v);
  }

  @Override public ISimulatedFinancialDataSymbolListOutput getSymbolList() {

    if (!isConnected) {  // contract: return null if not connected
      return null;
    }

    long now = System.currentTimeMillis();

    if (now - lastConfigurationEmittion >= 10000 || lastConfigurationEmittion == 0) {
      lastConfigurationEmittion = now;
      ISimulatedFinancialDataSymbolListOutput allSymbols =
        new SimulatedFinancialData.SimulatedFinancialDataSymbolListOutput();
      allSymbols.setAllSymbols(allSymbolsList);
      return allSymbols;
    }
    return null;
  }

  @Override public String getAggregationKey(ISimulatedFinancialDataSymbolListOutput tuple) {
    return null;
  }

  @Override public void connect() throws DefaultModeException {
    if (isConnected) { // contract: ignore re-connects
        return;
    }
    logger.info("Connecting...");

    financialMonitoringTimestamp = 0L;
    finacialThroughput = 0L;
    measurementDuration = 1 * 60;
    speedFactor = 1.0;
    prevTimeStampNow = 0;
    lastConfigurationEmittion = 0;
    idsToNamesMap = new HashMap<>();
    startSleeping = 0;
    allSymbolsList = new ArrayList<>();

    // Load allSymbols file
    if (useHdfs) {
      hdfsConfig = new Configuration();
      hdfsConfig.set("fs.defaultFS", hdfsUrl);
      hdfsConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      hdfsConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      try {
        fs = FileSystem.get(hdfsConfig);
        Path hdfsPathToSymbolList = new Path(pathToSymbolList);
        brForList = new BufferedReader(new InputStreamReader(fs.open(hdfsPathToSymbolList)));
      } catch (IOException e) {
        logger.error("Simulator Error : " + e.getMessage());
      }
    } else {
      fileForList = new File(pathToSymbolList);
      try {
        brForList = new BufferedReader(new FileReader(fileForList));
      } catch (FileNotFoundException e) {
        logger.error("Simulator Error : " + e.getMessage());
      }
    }

    // Add allSymbols to list
    String line;

    try {
      while ((line = brForList.readLine()) != null) {
        //        String symbolId = value.replace((char) 65533, (char) 183);
        allSymbolsList.add(line.replace((char) 65533, (char) 183));
      }
    } catch (IOException e) {
      logger.error("Simulator Error : " + e.getMessage());
      throw new DefaultModeException("Simulator Error : " + e.getMessage());
    }

    if (useHdfs) {
      try {
        Path hdfsPathToData = new Path(pathToData);
        brForData = new BufferedReader(new InputStreamReader(fs.open(hdfsPathToData)));
      } catch (IOException e) {
        logger.error("Simulator Error : " + e.getMessage());
      }
    } else {
      fileForData = new File(pathToData);
      try {
        brForData = new BufferedReader(new FileReader(fileForData));
      } catch (FileNotFoundException e) {
        logger.error("Simulator Error : " + e.getMessage());
        throw new DefaultModeException("Simulator Error : " + e.getMessage());
      }
    }

    // Read first line from data file to get the timestamp offset
    try {
      if ((lineRead = brForData.readLine()) != null) {
        DateTime now = new DateTime();
        //split data
        String ar[] = lineRead.split(",");
        String dateStr = ar[1] + " " + ar[2];
        //end split
        try {
          DateTime symbolTimeStamp = new DateTime(sdf.parse(dateStr)
                                                     .getTime());
          offsetInMillis = now.getMillis() - symbolTimeStamp.getMillis();
        } catch (ParseException e) {
          logger.error("Simulator Error : " + e.getMessage());
        }
      }
    } catch (IOException e) {
      logger.error("Simulator Error : " + e.getMessage());
      throw new DefaultModeException("Simulator Error : " + e.getMessage());
    }

    shouldReadNextLine = true;
    for (String s : allSymbolsList) {
      idsToNamesMap.put(s, s);
    }
    if (mappingChangedListener != null) {
      mappingChangedListener.notifyIdsNamesMapChanged();
    }
    isConnected = true;
  }

  @Override public void disconnect() {
    if (!isConnected) { // contract: ignore re-disconnects
        return;
    }
    closeQuietly(brForList);
    brForList = null;
    allSymbolsList = null;
    idsToNamesMap = null;

    closeQuietly(brForData);
    brForData = null;
  }

  public void setSpeed(double speedFactor) {
    this.speedFactor = speedFactor;
  }

  public IStorageStrategyDescriptor getStrategy() {
    return null;
  }

  public void setStrategy(IStorageStrategyDescriptor iStorageStrategyDescriptor) {

  }

  public Double getMeasurement(IObservable iObservable) {
    return null;
  }

  @Override public IHistoricalDataProvider getHistoricalDataProvider() {
      return new SpringHistoricalDataProvider();
  }

  @Override public Map<String, String> getIdsNamesMap() {
      // for demo scenario
      HashMap<String, String> map = new HashMap<String, String>();
      map.put("1656", "NASDAQ·NFLX·NoExpiry");
      return map;
  }

  @Override public void setDataSourceListener(IDataSourceListener iDataSourceListener) {
    mappingChangedListener = iDataSourceListener;
    mappingChangedListener.notifyIdsNamesMapChanged();
  }

  private void monitorMe() {
    if (financialMonitoringTimestamp == 0) {
      financialMonitoringTimestamp = System.currentTimeMillis();
      ++finacialThroughput;
    } else {
      long now = System.currentTimeMillis();
      if (now - financialMonitoringTimestamp < measurementDuration * 1000) {
        ++finacialThroughput;
      } else {
        logger.info(
          "Pipeline financial input throughput: " + ((double) finacialThroughput / (double) measurementDuration)
            + " tuples/sec");
        financialMonitoringTimestamp = now;
        finacialThroughput = 1;
      }
    }
  }

  private void closeQuietly(Closeable closeable) {
    if (null != closeable) {
      try {
        closeable.close();
      } catch (IOException e) {
      }
    }
  }
}
