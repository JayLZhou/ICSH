package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

/**
 * @author fangyixiang
 * @date Sep 17, 2018 read all the data
 */
public class DataReader {
    private String graphFile = null;
    private String vertexFile = null;
    private String edgeFile = null;
    private String weightFile = null;
    private int vertexNum = 0;
    private int edgeNum = 0;

    public DataReader(String graphFile, String vertexFile, String edgeFile, String weightFile) {
        this.graphFile = graphFile;
        this.vertexFile = vertexFile;
        this.edgeFile = edgeFile;
        this.weightFile = weightFile;

        // compute the number of nodes
        try {
            File test = new File(graphFile);
            long fileLength = test.length();
            LineNumberReader rf = new LineNumberReader(new FileReader(test));
            if (rf != null) {
                rf.skip(fileLength);
                vertexNum = rf.getLineNumber();// obtain the number of nodes
            }
            rf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // return the graph edge information
    public int[][] readGraph() {
        int graph[][] = new int[vertexNum][];
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(graphFile));

            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split(" ");
                int vertexId = Integer.parseInt(s[0]);

                int nb[] = new int[s.length - 1];
                for (int i = 1; i < s.length; i++)
                    nb[i - 1] = Integer.parseInt(s[i]);
                graph[vertexId] = nb;

                edgeNum += nb.length / 2;
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(graphFile + " |V|=" + vertexNum + " |E|=" + edgeNum / 2);// each edge is bidirectional

        return graph;
    }

    // return the type of each vertex
    public int[] readVertexType() {
        int vertexType[] = new int[vertexNum];

        try {
            BufferedReader stdin = new BufferedReader(new FileReader(vertexFile));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split(" ");
                int id = Integer.parseInt(s[0]);
                int type = Integer.parseInt(s[1]);
                vertexType[id] = type;
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return vertexType;
    }

    // return the type of each edge
    public int[] readEdgeType() {
        int edgeType[] = new int[edgeNum];

        try {
            BufferedReader stdin = new BufferedReader(new FileReader(edgeFile));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split(" ");
                int id = Integer.parseInt(s[0]);
                int type = Integer.parseInt(s[1]);
                edgeType[id] = type;
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return edgeType;
    }

    public double[] readWeight() {
        double weight[] = new double[vertexNum];

        try {
            BufferedReader stdin = new BufferedReader(new FileReader(weightFile));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split("\\s+");
                int id = Integer.parseInt(s[0]);
                double influence = Double.parseDouble(s[1]);
                weight[id] = influence;
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return weight;
    }
}
