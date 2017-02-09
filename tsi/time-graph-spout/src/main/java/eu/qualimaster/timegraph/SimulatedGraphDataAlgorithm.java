package eu.qualimaster.timegraph;

import eu.qualimaster.data.imp.SimulatedGraphData;
import eu.qualimaster.data.inf.ISimulatedGraphData;
import eu.qualimaster.dataManagement.DataManagementConfiguration;
import eu.qualimaster.dataManagement.sources.IDataSourceListener;
import eu.qualimaster.dataManagement.sources.IHistoricalDataProvider;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.pipeline.DefaultModeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created by ap0n on 8/2/2017.
 */
public class SimulatedGraphDataAlgorithm implements ISimulatedGraphData {

  private Logger logger = LoggerFactory.getLogger(SimulatedGraphDataAlgorithm.class);

  // For HDFS
  Configuration hdfsConfig;
  FileSystem fs;
  private boolean useHdfs = true;
  private String hdfsUrl = "";
  // /For HDFS
  private String pathToData;
  private File fileForData;
  private BufferedReader brForData;
  private boolean done;
  private boolean isConnected;

  public SimulatedGraphDataAlgorithm() {
    String dataFilename = "data.txt";
    String pathPrefix;
    useHdfs = DataManagementConfiguration.useSimulationHdfs();

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
    pathToData = pathPrefix + "/graphData/" + dataFilename;
    logger.info("Path to data.txt: " + pathToData);

    done = false;
    isConnected = false;
  }

  @Override
  public ISimulatedGraphDataEdgeStreamOutput getEdgeStream() {
    if (!isConnected || done) {
      return null;
    }

    ISimulatedGraphDataEdgeStreamOutput result = null;
    String lineRead;
    try {
      if ((lineRead = brForData.readLine()) != null) {
        if (lineRead.startsWith(" ") || lineRead.equals("")) {
          done = true;
          logger.info("Dataset is over");
        } else {
          result = new SimulatedGraphData.SimulatedGraphDataEdgeStreamOutput();
          result.setEdge(lineRead);
        }
      }
    } catch (IOException e) {
      done = true;
      logger.info("Dataset is over");
    }

    return result;
  }

  @Override
  public String getAggregationKey(ISimulatedGraphDataEdgeStreamOutput tuple) {
    return null;
  }

  @Override
  public IHistoricalDataProvider getHistoricalDataProvider() {
    return null;
  }

  @Override
  public Map<String, String> getIdsNamesMap() {
    return null;
  }

  @Override
  public void setDataSourceListener(IDataSourceListener iDataSourceListener) {

  }

  @Override
  public void connect() {
    if (isConnected) { // contract: ignore re-connects
      return;
    }
    logger.info("Connecting...");

    // Load allSymbols file
    if (useHdfs) {
      hdfsConfig = new Configuration();
      hdfsConfig.set("fs.defaultFS", hdfsUrl);
      hdfsConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      hdfsConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      try {
        fs = FileSystem.get(hdfsConfig);
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
    isConnected = true;
  }

  @Override
  public void disconnect() {
    if (!isConnected) { // contract: ignore re-disconnects
      return;
    }
    closeQuietly(brForData);
    brForData = null;
  }

  @Override
  public void setStrategy(IStorageStrategyDescriptor iStorageStrategyDescriptor) {

  }

  @Override
  public IStorageStrategyDescriptor getStrategy() {
    return null;
  }

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return null;
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
