package util;

import javafx.util.Pair;

import java.io.*;
import java.util.*;

/**
 * @author fangyixiang
 * @date Sep 17, 2018 read all the data
 */
public class MusicCreate {
    public String rootPath = "/Users/zhouxiaolun/MasterLibrary/graphaTherory/InfluentialCommunities/dataset/music";
    public String dst_root = "/Users/zhouxiaolun/MasterLibrary/graphaTherory/InfluentialCommunities/dataset/music";
    public Map<Integer, Integer> id2type = new HashMap<>();
    public Map<Integer, Integer> type2cnt = new HashMap<>();
    public ArrayList<Map<Integer, Set<Integer>>> allMap = new ArrayList<>();
    public Map<Integer, Set<Integer>> pnbMap = new HashMap<>();


    int[][] graph = null; // for create graph.txt;
    String[] nodeVertex = new String[]{"artist", "release", "term", "song"};

    private void readAllNode() {
        try {
            int cnt = 0;
            BufferedWriter bufferedWriterVertex = new BufferedWriter(new FileWriter(dst_root + "/" + "vertex.txt"));
            BufferedWriter bufferedWriterWeight = new BufferedWriter(new FileWriter(dst_root + "/" + "weight.txt"));

            for (int i = 0; i < nodeVertex.length; i++) {
                String nodePath = rootPath + "/" + "node_importance-" + nodeVertex[i] + ".txt";
                BufferedReader stdin = new BufferedReader(new FileReader(nodePath));
                String line = null;
                while ((line = stdin.readLine()) != null) {
                    String s[] = line.split("\\s+");
                    if (type2cnt.containsKey(i)) {
                        Integer count = type2cnt.get(i);
                        type2cnt.replace(i, count + 1);
                    } else {
                        type2cnt.put(i, 1);
                    }
                    bufferedWriterVertex.write(Integer.toString(cnt) + " " + Integer.toString(i) + "\n");
                    bufferedWriterWeight.write(Integer.toString(cnt) + " " + s[1] + "\n");
                    id2type.put(cnt, i);
                    cnt += 1;
                }
            }
            bufferedWriterVertex.close();
            bufferedWriterWeight.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getAllMap() {
    }

    public static void main(String[] args) throws IOException {
        // artist 0
        // release 1
        // term 2
        // song 3
        // a-r 0
        // r-a 1
        // a-t 2
        // t-a 3
        // s-a 4
        // a-s 5
        // s-r 6
        // r-s 7
        MusicCreate graphCreater = new MusicCreate();
        graphCreater.readAllNode();
        graphCreater.createGraph();

    }

    private void createGraph() throws IOException {
        int cnt = 0;
        for (int i = 0; i < nodeVertex.length; i++) {
            int vertexId = i;
            String left = nodeVertex[i];

            for (int j = 0; j < nodeVertex.length; j++) {
                if (i == j) {
                    continue;
                }
                String right = nodeVertex[j];
                String edgePath = rootPath + "/" + left + "-" + right + ".txt";
                FileReader reader;
                try {
                    reader = new FileReader(edgePath);
                } catch (FileNotFoundException e) {
                    continue;
                }
                System.out.println("edgePath is : " + edgePath);

                BufferedReader stdin = new BufferedReader(reader);
                String line = null;
                while ((line = stdin.readLine()) != null) {
                    String s[] = line.split("\\s+");
                    int leftId = getVertexId(Integer.parseInt(s[0]), i);
                    int rightId = getVertexId(Integer.parseInt(s[1]), j);
                    if (pnbMap.containsKey(leftId)) {
                        pnbMap.get(leftId).add(rightId);
                    } else {
                        Set<Integer> tmpSet = new HashSet<>();
                        tmpSet.add(rightId);
                        pnbMap.put(leftId, tmpSet);
                    }

                    //
                    if (pnbMap.containsKey(rightId)) {
                        pnbMap.get(rightId).add(leftId);
                    } else {
                        Set<Integer> tmpSet = new HashSet<>();
                        tmpSet.add(leftId);
                        pnbMap.put(rightId, tmpSet);
                    }
                }
            }
        }
        System.out.println("build map over ");
        BufferedWriter bufferedWriterEdge = new BufferedWriter(new FileWriter(dst_root + "/" + "edge.txt"));
        BufferedWriter bufferedWriterGraph = new BufferedWriter(new FileWriter(dst_root + "/" + "graph.txt"));
        int len = pnbMap.keySet().size();
        int cntEdge = 0;
        for (int key = 0; key < len; key++) {
            bufferedWriterGraph.write(Integer.toString(key));
            for (int val : pnbMap.get(key)) {
                bufferedWriterGraph.write(" " + val + " " + Integer.toString(cntEdge));
                bufferedWriterEdge.write(Integer.toString(cntEdge) + " " + Integer.toString(getEdgeType(key, val)) + "\n");
                cntEdge += 1;
            }
            bufferedWriterGraph.write("\n");
        }
        bufferedWriterEdge.close();
        bufferedWriterGraph.close();
    }

    private int getEdgeType(int left, int right) {
        int leftType = id2type.get(left);
        int rightType = id2type.get(right);

        return getEdgeId(leftType, rightType);
    }


    private int getVertexId(int id, int i) {
        int offset = 0;
        for (int k = 0; k < i; k++) {
            offset += type2cnt.get(k);
        }
        return id + offset;

    }

    private int getEdgeId(int i, int j) {
        //     String[] nodeVertex = new String[]{"artist", "release", "term", "song"};
        // a-r 0
        // r-a 1
        // a-t 2
        // t-a 3
        // s-a 4
        // a-s 5
        // s-r 6
        // r-s 7
        if (i == 0) {
            if (j == 1) {
                // a-r
                return 0;
            }
            if (j == 2) {
                // a-t
                return 2;
            }
            if (j == 3) {
                //a-s
                return 5;
            }
        }
        if (i == 1) {
            if (j == 0) {
                //r-a
                return 1;
            }
            if (j == 3) {
                //r-s
                return 7;
            }
        }
        if (i == 2) {
            if (j == 0) {
                // t - a
                return 3;
            }
        }
        if (i == 3) {
            if (j == 0) {
                // s-a
                return 4;
            }
            if (j == 1) {
                return 6;
            }
        }
        return -1;
    }

    private void analysize() {

    }


}