package main.java.dijkstra;


import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

/**
 * 解析value的对象。value：0			B:7	C:9	G:14
 * @author root
 *
 */
public class Node implements Serializable {
  public static int INFINITE = Integer.MAX_VALUE;
  private int distance = INFINITE;
  //上一个节点
  private String backpointer;
  //与该节点相邻的其他节点数组
  private String[] adjacentNodeNames;

  public static final char fieldSeparator = '\t';

  public int getDistance() {
    return distance;
  }

  public Node setDistance(int distance) {
    this.distance = distance;
    return this;
  }

  public String getBackpointer() {
    return backpointer;
  }

  public Node setBackpointer(String backpointer) {
    this.backpointer = backpointer;
    return this;
  }

  /**
   * A -> B
   * @param name
   * @return
   */
  public String constructBackpointer(String name) {
    StringBuilder backpointers = new StringBuilder();
    if (StringUtils.trimToNull(getBackpointer()) != null) {
      backpointers.append(getBackpointer()).append("->");
    }
    backpointers.append(name);
    return backpointers.toString();
  }

  public String[] getAdjacentNodeNames() {
    return adjacentNodeNames;
  }

  public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
    this.adjacentNodeNames = adjacentNodeNames;
    return this;
  }

  public boolean containsAdjacentNodes() {
    return adjacentNodeNames != null;
  }

  public boolean isDistanceSet() {
    return distance != INFINITE;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(distance)
        .append(fieldSeparator)
        .append(backpointer);

    if (getAdjacentNodeNames() != null) {
      sb.append(fieldSeparator)
          .append(StringUtils
              .join(getAdjacentNodeNames(), fieldSeparator));
    }
    return sb.toString();
  }

  public static Node fromMR(String value) throws IOException {
    String[] parts = StringUtils.splitPreserveAllTokens(
        value, fieldSeparator);
    if (parts.length < 2) {
      throw new IOException(
          "Expected 2 or more parts but received " + parts.length);
    }
    Node node = new Node()
        .setDistance(Integer.valueOf(parts[0]))
        .setBackpointer(StringUtils.trim(parts[1]));
    if (parts.length > 2) {
      node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 2,
          parts.length));
    }
    return node;
  }
}