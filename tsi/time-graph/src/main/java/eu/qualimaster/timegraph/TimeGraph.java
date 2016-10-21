package eu.qualimaster.timegraph;

import eu.qualimaster.families.inf.IFTimeGraph;
import eu.qualimaster.observables.IObservable;
import eu.qualimaster.pipeline.DefaultModeException;

import org.apache.commons.lang.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gr.tuc.softnet.ap0n.graph.Edge;
import gr.tuc.softnet.ap0n.graph.Snapshot;
import gr.tuc.softnet.ap0n.graph.Vertex;
import gr.tuc.softnet.ap0n.index.VolatileIndex;

/**
 * Created by ap0n on 22/7/2016.
 */
public class TimeGraph implements IFTimeGraph {

  final static Logger logger = LoggerFactory.getLogger(TimeGraph.class);
  private VolatileIndex index;
  private Set<String>
      vertices;
      // TODO: Remove me as soon as the index supports adding edges with new vertices
  String previousValue;

  public TimeGraph() {
    previousValue = "";
    vertices = new HashSet<>();
    try {
      index = new VolatileIndex();
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  @Override
  public void calculate(IIFTimeGraphEdgeStreamInput input,
                        IIFTimeGraphSnapshotStreamOutput snapshotStreamResult,
                        IIFTimeGraphPathStreamOutput pathStreamResult) {
    // Avoid getting the same tuple multiple times
    if (previousValue.equals(input.getEdge())) {
      return;
    } else {
      previousValue = input.getEdge();
    }

    // No result from here
    snapshotStreamResult.noOutput();
    pathStreamResult.noOutput();

    // input = (v0,v1,MM/dd/yyyy,HH:mm:ss,1 or v0,v1,MM/dd/yyyy,HH:mm:ss,0). 1 is addition, 0 is deletion
    String[] in = input.getEdge().split(",");
    String vertex0 = in[0];
    String vertex1 = in[1];
    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
    Date date;
    Long time = 0L;
    boolean isAddition = in[4].equals("1");
    try {
      date = dateFormat.parse(in[2] + "," + in[3]);
      time = date.getTime();
    } catch (ParseException e) {
      logger.error(e.getMessage(), e);
      throw new DefaultModeException(e.getMessage(), e);
    }

    for (int i = 0; i < 2; i++) {  // Add vertex0 & vertex1 to the index (if not already in)
      String vertex = in[i];
      try {
        if (!vertices.contains(vertex)) {
          Vertex v = new Vertex(vertex);
//          logger.info("Adding vertex: " + v.toString());
          index.addGraphNode(v);
          vertices.add(vertex);
        }
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }

    Edge edge = new Edge(vertex0, vertex1, System.currentTimeMillis());
    if (isAddition) {
      // add edge to index
      try {
//        logger.info("Adding edge: " + edge.toString());
        index.addGraphEdge(edge);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    } else {
      // delete edge from index
      try {
//        logger.info("Expiring edge: " + edge.toString());
        index.expireEdge(edge);
      } catch (Exception e) {
//        logger.warn(e.getMessage());
      }
    }
  }

  @Override
  public void calculate(IIFTimeGraphSnapshotQueryStreamInput input,
                        IIFTimeGraphSnapshotStreamOutput snapshotStreamResult,
                        IIFTimeGraphPathStreamOutput pathStreamResult) {
    // No path result here
    pathStreamResult.noOutput();

    long start = input.getStart();
    long end = input.getEnd();

    if (start == -1 || end == -1) {
      snapshotStreamResult.noOutput();
      return;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("snapshots:[");
    try {
      logger.info("calculating snapshots for interval [" + start + "," + end + ")");
      List<Snapshot> snapshotList = index.getSnapshots(start, end);
      logger.info("snapshots calculated");

      for (Snapshot s : snapshotList) {
        sb.append(s.toJsonString()).append(",");
      }
      if (snapshotList.size() > 0) {
        sb.setLength(sb.length() - 1);
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
    sb.append("]");
    snapshotStreamResult.clear();
    snapshotStreamResult.setSnapshot(sb.toString());
  }

  @Override
  public void calculate(IIFTimeGraphPathQueryStreamInput input,
                        IIFTimeGraphSnapshotStreamOutput snapshotStreamResult,
                        IIFTimeGraphPathStreamOutput pathStreamResult) {
    snapshotStreamResult.noOutput();
    pathStreamResult.noOutput();
    // TODO: Implement me
  }

  @Override
  public void switchState(State state) {

  }

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return null;
  }
}
