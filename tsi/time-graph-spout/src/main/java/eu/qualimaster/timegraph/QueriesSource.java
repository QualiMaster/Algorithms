package eu.qualimaster.timegraph;

import eu.qualimaster.data.imp.TimeGraphQueriesSource;
import eu.qualimaster.data.inf.ITimeGraphQueriesSource;
import eu.qualimaster.dataManagement.sources.IDataSourceListener;
import eu.qualimaster.dataManagement.sources.IHistoricalDataProvider;
import eu.qualimaster.dataManagement.strategies.IStorageStrategyDescriptor;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.pipeline.DefaultModeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Created by ap0n on 26/7/2016.
 */
public class QueriesSource implements ITimeGraphQueriesSource {

  static final Logger logger = LoggerFactory.getLogger(QueriesSource.class);
  private long start;
  private long end;
  private String pathQuery;

  public QueriesSource() {
    this.start = -1;
    this.end = -1;
    this.pathQuery = "";
  }

  @Override
  public ITimeGraphQueriesSourceSnapshotQueryStreamOutput getSnapshotQueryStream() {
    if (start != -1 && end != -1) {
      logger.info("Sending snapshots query");
      TimeGraphQueriesSource.TimeGraphQueriesSourceSnapshotQueryStreamOutput
          o =
          new TimeGraphQueriesSource.TimeGraphQueriesSourceSnapshotQueryStreamOutput();
      o.setStart(start);
      o.setEnd(end);
      start = -1;
      end = -1;
      return o;
    }

    try {
      // TODO: Replace with condition variables (if set parameter runs on different thread)
      Thread.sleep(250);
    } catch (InterruptedException e) {
      logger.warn(e.getMessage(), e);
    }

    return null;
  }

  @Override
  public String getAggregationKey(
      ITimeGraphQueriesSourceSnapshotQueryStreamOutput iTimeGraphQueriesSourceSnapshotQueryStreamOutput) {
    return null;
  }

  @Override
  public ITimeGraphQueriesSourcePathQueryStreamOutput getPathQueryStream() {
    if (!pathQuery.equals("")) {
      TimeGraphQueriesSource.TimeGraphQueriesSourcePathQueryStreamOutput
          o = new TimeGraphQueriesSource.TimeGraphQueriesSourcePathQueryStreamOutput();
      o.setQuery(pathQuery);
      pathQuery = "";
      return o;
    }

    try {
      Thread.sleep(250);
    } catch (InterruptedException e) {
      logger.warn(e.getMessage(), e);
    }

    return null;
  }

  @Override
  public String getAggregationKey(
      ITimeGraphQueriesSourcePathQueryStreamOutput iTimeGraphQueriesSourcePathQueryStreamOutput) {
    return null;
  }

  @Override
  // TODO: Send a parameter change (-1,-1) to init this (the same values sent twice won't reach the spout)
  public void setParameterSnapshotQuery(String value) {
    logger.info("Snapshot query received: " + value);
    if (value.equals("")) {
      start = -1;
      end = -1;
      return;
    }
    // value = "start,end" format = (MM/dd/yyyy,HH.mm.ss,MM/dd/yyyy,HH.mm.ss)
    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy,HH.mm.ss");
    try {
      String[] params = value.split(",");
      start = dateFormat.parse(params[0] + "," + params[1]).getTime();
      // TODO: Only "stubs" are currently supported
//      end = dateFormat.parse(params[2] + "," + params[3]).getTime();
      end = start;
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new DefaultModeException(e.getMessage(), e);
    }
  }

  @Override
  // TODO: Send a parameter change (-1,-1) to init this (the same values sent twice won't reach the spout)
  public void setParameterPathQuery(String value) {
    logger.info("Path query received: " + value);
    pathQuery = value;
  }

  @Override
  public void connect() {
    // Do nothing here
  }

  @Override
  public void disconnect() {
    // Do nothing here
  }

  @Override
  public IStorageStrategyDescriptor getStrategy() {
    return null;
  }

  @Override
  public void setStrategy(IStorageStrategyDescriptor iStorageStrategyDescriptor) {

  }

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return null;
  }

  @Override
  public IHistoricalDataProvider getHistoricalDataProvider() {
    return null;
  }

  @Override
  public Map<String, String> getIdsNamesMap() {
    return null;  // Means nothing for this pipeline
  }

  @Override
  public void setDataSourceListener(IDataSourceListener iDataSourceListener) {
    // Means nothing for this pipeline
  }
}