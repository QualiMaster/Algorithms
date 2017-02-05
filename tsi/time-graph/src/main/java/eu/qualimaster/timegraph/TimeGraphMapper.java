package eu.qualimaster.timegraph;

import eu.qualimaster.families.inf.IFTimeGraphMapper;
import eu.qualimaster.observables.IObservable;

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
import gr.tuc.softnet.ap0n.graph.Vertex;

import static utils.HashFunctions.he;
import static utils.HashFunctions.hv;

/**
 * Created by ap0n on 25/11/2016.
 */
public class TimeGraphMapper implements IFTimeGraphMapper {
  private Logger logger = LoggerFactory.getLogger(TimeGraphMapper.class);
  private List<Integer> timeGraphTaskIds;
  private Set<String> indexedVertexIds;

  public TimeGraphMapper(List<Integer> timeGraphTaskIds, int myId) {
    this.timeGraphTaskIds = timeGraphTaskIds;
    this.indexedVertexIds = new HashSet<>();
  }

  public List<Integer> getTimeGraphTaskIds() {
    return timeGraphTaskIds;
  }

  public void setTimeGraphTaskIds(List<Integer> timeGraphTaskIds) {
    this.timeGraphTaskIds = timeGraphTaskIds;
  }

  @Override
  public void calculate(IIFTimeGraphMapperEdgeStreamInput input,
                        IIFTimeGraphMapperDataStreamOutput dataStreamResult) {
    //  edge format: v0,v1,date,time,1 (addition) or v0,v1,date,time,0 (removal)
    // fields[0]: v0
    // fields[1]: v1
    // fields[2]: MM/dd/yyyy
    // fields[3]: HH:mm:ss
    // fields[4]: 1 (addition) / 0 (deletion)

    dataStreamResult.clear();
    String edge = input.getEdge();

    if (edge == null) {
      logger.warn("Null input.edge");
      dataStreamResult.noOutput();
      return;
    } else {
      logger.info("Not null input.edge");
    }

    String[] fields = edge.split(",");
    DateFormat df = new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
    Date date = null;
    try {
      date = df.parse(fields[2] + "," + fields[3]);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    long time = date.getTime();

    if (fields[4].equals("0")) {
      // Deletion
      Edge toDelete = new Edge(fields[0], fields[1], time);
      logger.info("Deleting edge: " + toDelete.toString());
      emitDirect(toDelete, false, dataStreamResult);
    } else {
      // Addition
      if (indexedVertexIds.add(fields[0])) {
        // Add v0
        Vertex toAdd = new Vertex(fields[0], time);
        logger.info("Adding vertex: " + toAdd.toString());
        emitDirect(toAdd, true, dataStreamResult);
      }
      if (indexedVertexIds.add(fields[1])) {
        // Add v1
        Vertex toAdd = new Vertex(fields[1], time);
        logger.info("Adding vertex: " + toAdd.toString());
        emitDirect(toAdd, true, dataStreamResult);
      }
      // Add edge
      Edge toAdd = new Edge(fields[0], fields[1], time);
      logger.info("Adding edge: " + toAdd.toString());
      emitDirect(toAdd, true, dataStreamResult);
    }
  }

  @Override
  public void switchState(State state) {

  }

  @Override
  public Double getMeasurement(IObservable iObservable) {
    return null;
  }

  private void emitDirect(Object o, boolean isAddition, IIFTimeGraphMapperDataStreamOutput out) {
    IIFTimeGraphMapperDataStreamOutput result = out.createItem();
    result.setIsAddition(isAddition);
    result.setUpdate(o);
    int taskId = (o instanceof Vertex) ? hv((Vertex) o, timeGraphTaskIds)
                                       : he((Edge) o, timeGraphTaskIds);
    result.setTaskId(taskId);
    out.emitDirect("nothing here", result);
  }
}
