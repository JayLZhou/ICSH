package util;

import basic.InfComm2Type;
import javafx.util.Pair;

import java.io.*;
import java.rmi.server.ExportException;
import java.util.*;

class tpoicComparator implements Comparator<Integer> {
    private Map<Integer, Integer> topic2cnt;

    public tpoicComparator(Map<Integer, Integer> topic2cnt) {
        this.topic2cnt = topic2cnt;
    }

    //
    public int compare(Integer o1, Integer o2) {
        int valA = topic2cnt.get(o1);
        int valB = topic2cnt.get(o2);
        return valA >= valB ? -1 : 1;
    }
};

/**
 * @author xiaolunzhou
 * @date Sep 17, 2018 read all the data
 */
public class GraphCreater {
    public String rootPath = "/Users/zhouxiaolun/MasterLibrary/graphaTherory/InfluentialCommunities/dataset/DBLPWEIGHT";
    public String dst_root = "/Users/zhouxiaolun/MasterLibrary/graphaTherory/InfluentialCommunities/dataset/DBLPWithWeight";

    public Map<Integer, Set<Integer>> paper2Venue = new HashMap<>();
    public Map<Integer, Set<Integer>> year2Paper = new HashMap<>();
    public Map<Integer, Integer> Paper2Year = new HashMap<>();
    public Map<Integer, Double> venueWeight = new HashMap<>();
    public Map<Integer, Double> newVenue2Weight = new HashMap<>();
    public Map<Integer, Double> paper2Weight = new HashMap<>();
    public Map<Integer, Double> fos2Weight = new HashMap<>();
    public Map<Integer, Double> author2Weight = new HashMap<>();
    public Map<Integer, Set<Integer>> paper2Author = new HashMap<>();
    public Map<Integer, Set<Integer>> paper2topic = new HashMap<>();
    public Map<Integer, Set<Integer>> Author2Paper = new HashMap<>();
    public Map<Integer, Map<Integer, Integer>> venueyear2newID = new HashMap<>();
    public Set<Integer> deletePaper = new HashSet<>();
    public Map<Integer, Double> id2Double = new HashMap<>(); // for create weight.txt
    public Map<Integer, Integer> id2Type = new HashMap<>(); // for create vertex.txt;
    public Map<Integer, Integer> edgeId2Type = new HashMap<>(); // for create edge.txt;
    public Map<Integer, Set<Integer>> venue2Year = new HashMap<>();
    public Map<Integer, Set<Integer>> pnbMap = new HashMap<>();
    public Map<Integer, Map<Integer, Integer>> author2topic = new HashMap<>();
    public Map<Integer, List<Integer>> authorSortedTopic = new HashMap<>();
    public Map<Integer, Integer> topic2paper = new HashMap<>();
    public List<Integer> sortTopic = new ArrayList<>();
    public Set<Integer> nonTopic = new HashSet<>();
    public Set<Integer> deleteSet = new HashSet<>();

    int[][] graph = null; // for create graph.txt;


    public void getAllMap() {
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(rootPath + "/" + "paper-venue.txt"));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split("\\s+");
                int paperId = Integer.parseInt(s[0]);
                int venueId = Integer.parseInt(s[1]);
                if (paper2Venue.containsKey(paperId)) {
                    paper2Venue.get(paperId).add(venueId);
                } else {
                    Set<Integer> tmpSet = new HashSet<>();
                    tmpSet.add(venueId);
                    paper2Venue.put(paperId, tmpSet);
                }
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // find venue -> weight
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(rootPath + "/" + "venue.txt"));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split("\\s+");
                int venueId = Integer.parseInt(s[0]);
                Double weight = Double.parseDouble(s[1]);
                venueWeight.put(venueId, weight);
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // find paper -> weight
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(rootPath + "/" + "paper.txt"));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split("\\s+");
                int paperId = Integer.parseInt(s[0]);
                Double weight = Double.parseDouble(s[1]);
                paper2Weight.put(paperId, weight);
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // find author -> weight
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(rootPath + "/" + "author.txt"));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split("\\s+");
                int authorId = Integer.parseInt(s[0]);
                Double weight = Double.parseDouble(s[1]);
                author2Weight.put(authorId, weight);
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // find fos -> weight
        int noncnt = 0;
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(rootPath + "/" + "fos.txt"));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split("\\s+");
                String inf = "\\N";
                if (s[1].equals(inf)) {
                    s[1] = "0";
                    nonTopic.add(Integer.parseInt(s[0]));
                    System.out.println(s[0]);
                    noncnt += 1;
                }
                int fosId = Integer.parseInt(s[0]);
                double weight = Integer.parseInt(s[1]);

                fos2Weight.put(fosId, weight);
            }
            stdin.close();
            System.out.println("all cnt is :" + noncnt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // find year -> paper
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(rootPath + "/" + "paper-year.txt"));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split("\\s+");
                int paperId = Integer.parseInt(s[0]);
                int yearId = Integer.parseInt(s[1]);
                Paper2Year.put(paperId, yearId);
                if (year2Paper.containsKey(yearId)) {
                    year2Paper.get(yearId).add(paperId);
                } else {
                    Set<Integer> tmpSet = new HashSet<>();
                    tmpSet.add(paperId);
                    year2Paper.put(yearId, tmpSet);
                }
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (int year : year2Paper.keySet()) {
            Set<Integer> paperSet = year2Paper.get(year);
            for (int paper : paperSet) {
                Set<Integer> venueSet = paper2Venue.get(paper);
                for (int venue : venueSet) {
                    if (venue2Year.containsKey(venue)) {
                        venue2Year.get(venue).add(year);
                    } else {
                        Set<Integer> tmpSet = new HashSet<>();
                        tmpSet.add(year);
                        venue2Year.put(venue, tmpSet);
                    }
                }
            }
        }
        // map : paper <-> author
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(rootPath + "/" + "paper-author.txt"));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split("\\s+");
                int paperId = Integer.parseInt(s[0]);
                int authorId = Integer.parseInt(s[1]);
                if (paper2Author.containsKey(paperId)) {
                    paper2Author.get(paperId).add(authorId);
                } else {
                    Set<Integer> tmpSet = new HashSet<>();
                    tmpSet.add(authorId);
                    paper2Author.put(paperId, tmpSet);
                }
                if (Author2Paper.containsKey(authorId)) {
                    Author2Paper.get(authorId).add(paperId);
                } else {
                    Set<Integer> tmpSet = new HashSet<>();
                    tmpSet.add(paperId);
                    Author2Paper.put(authorId, tmpSet);
                }
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        int cnt = 0;
        for (int venue : venue2Year.keySet()) {
            venueyear2newID.put(venue, new HashMap<>());
            for (int year : venue2Year.get(venue)) {
                venueyear2newID.get(venue).put(year, cnt);
                newVenue2Weight.put(cnt, venueWeight.get(venue)); // new VenueId -- weight
                cnt += 1;
            }
        }
        System.out.println("over");
//        for (int paper : Paper2Year.keySet()) {
//            int venue = paper2Venue.get(paper);
//            venueyear2newID.put(venue, new HashMap<>());
//            for (int year : venue2Year.get(venue)) {
//                venueyear2newID.get(venue).put(year, cnt);
//                newVenue2Weight.put(cnt, venueWeight.get(venue)); // new VenueId -- weight
//                cnt += 1;
//            }
//        }
    }

    public Integer getVertexType(int vertex) {
        if (vertex < 409340) {
            // paper
            return 0;
        }
        if (vertex >= 409340 && vertex < 811494) {
            return 1; // author
        }
        if (vertex >= 811494 && vertex < 811637) {
            return 2; // venue
        }
        if (vertex >= 811637 && vertex < 877656) {
            return 3; // fos
        }
        return -1; // year
    }

    public Integer getNewVertexType(int vertex) {
        if (vertex < 409340) {
            // paper
            return 0;
        }
        if (vertex >= 409340 && vertex < 811494) {
            return 1; // author
        }
        if (vertex >= 811494 && vertex < 815020) {
            return 2; // venue
        }
        if (vertex >= 815020 && vertex < 881039) {
            return 3; // fos
        }
        return -1; // year
    }

    public int getEdgeType(int leftId, int rightId) {
        int left = getNewVertexType(leftId);
        int right = getNewVertexType(rightId);
        if (left == 0 && right == 1) {
            return 0;
        }

        if (left == 0 && right == 2) {
            return 1;
        }

        if (left == 0 && right == 3) {
            return 2;
        }
        if (left == 1 && right == 0) {
            return 3;
        }
        if (left == 2 && right == 0) {
            return 4;
        }
        if (left == 3 && right == 0) {
            return 5;
        }
        return -1;
    }

    public double getWeight(int id) {
        int type = getNewVertexType(id);
        int offset = getOffset(id, type);
        if (type == 0) {
            return paper2Weight.get(offset);
        }
        if (type == 1) {
            return author2Weight.get(offset);
        }
        if (type == 2) {
            return newVenue2Weight.get(offset);
        }
        if (type == 3) {
            return fos2Weight.get(offset);
        }
        System.out.println("sdsdsdsdsdsdsdsdsdsdsd");
        return 0.0;
    }

    private int getOffset(int id, int type) {
        if (type == 0) {
            return id;
        }
        if (type == 1) {
            return id - 409340;
        }
        if (type == 2) {
            return id - 811494;
        }
        if (type == 3) {
            return id - 815020;
        }
        return -1;
    }

    public static void main(String[] args) throws IOException {
        GraphCreater graphCreater = new GraphCreater();
        graphCreater.getAllMap();
        graphCreater.createNewAllEdge();
        graphCreater.createAlltxt();
//        graphCreater.analysize();

    }

    private void analysize() {

    }


    private void createAlltxt() throws IOException {
        int cntGraph = 0;
        int cntEdge = 0;
        int cntVertex = 0;
        BufferedWriter bufferedWriterEdge = new BufferedWriter(new FileWriter(dst_root + "/" + "edge.txt"));
        BufferedWriter bufferedWriterWeight = new BufferedWriter(new FileWriter(dst_root + "/" + "weight.txt"));
        BufferedWriter bufferedWriterVertex = new BufferedWriter(new FileWriter(dst_root + "/" + "vertex.txt"));
        BufferedWriter bufferedWriterGraph = new BufferedWriter(new FileWriter(dst_root + "/" + "graph.txt"));
//        int len = pnbMap.keySet().size();
        int len = 881039;
        int maxSize = 0;
        int maxAuthorSize = 0;
        int allCnt = 0;
        int cntV = 0;
        List<Pair<Integer, Integer>> topic2Paper = new ArrayList<>();
        List<Pair<Integer, Integer>> Paper2topic = new ArrayList<>();
        for (int key = 0; key < len; key++) {
            int cnt = 0;
            if (key < 409340) {
                if (getWeight(key) == 0) {
                    cntV += 1;
                }
                for (int val : pnbMap.get(key)) {
                    if (getNewVertexType(val) == 3) {
                        cnt += 1;
                    }
                }
                int topK = 5;
//               // obtain the top5
                if (cnt > topK) {

                    Set<Integer> authorSet = paper2Author.get(key);
                    if (authorSet == null) {
                        System.out.println("paper is : " + key);

                    } else {
                        Set<Integer> deleteSet = new HashSet<>();
                        for (int val : pnbMap.get(key)) {
                            if (getNewVertexType(val) == 3) {
                                deleteSet.add(val);
                            }
                        }
                        pnbMap.get(key).removeAll(deleteSet);
                        List<Integer> conditionTopic = new ArrayList<>();
                        for (Integer author : authorSet) {
                            if (!authorSortedTopic.containsKey(author)) {
                                continue;
                            }
                            for (int i = 0; i < Integer.max(topK, authorSortedTopic.get(author).size()); i++) {
                                conditionTopic.add(authorSortedTopic.get(author).get(i));
                            }
                        }
                        conditionTopic.sort(new tpoicComparator(topic2paper));
                        Set<Integer> newTopicSet = getTopKSet(topK, deleteSet, conditionTopic);
                        Set<Integer> deletedTopic = new HashSet<>();
                        for (Integer oriTopic : deleteSet) {
                            if (!newTopicSet.contains(oriTopic)) {
                                deletedTopic.add(oriTopic);
                            }
                        }

                        pnbMap.get(key).addAll(newTopicSet);
                        for (int del : deletedTopic) {
                            pnbMap.get(del).remove(key);
                        }
                    }
                }
            }


            // 先处理点
            bufferedWriterVertex.write(Integer.toString(cntVertex) + " " + Integer.toString(getNewVertexType(key)) + "\n");
            bufferedWriterWeight.write(Integer.toString(cntVertex) + " " + Double.toString(getWeight(key)) + "\n");
            cntVertex += 1;
            if (deleteSet.contains(key)) {
                bufferedWriterGraph.write(Integer.toString(key) + "\n");
                continue;
            }
//            if (key == 815042) {
//                bufferedWriterGraph.write(Integer.toString(key) + "\n");
//                continue;
//            }
            if (pnbMap.isEmpty()) {
                bufferedWriterGraph.write(Integer.toString(key) + "\n");
                continue;
            }
            if (!pnbMap.containsKey(key)) {
                bufferedWriterGraph.write(Integer.toString(key) + "\n");
                continue;
            }
            if (key >= 815020 && key < 881039) {
                topic2Paper.add(new Pair<>(key, pnbMap.get(key).size()));
            }
            // 处理边和graph
            bufferedWriterGraph.write(Integer.toString(key));
            for (int val : pnbMap.get(key)) {
//                if (deleteSet.contains(val) && val == 815042) {
//                    continue;
//                }
                                if (deleteSet.contains(val) ) {
                    continue;
                }
                bufferedWriterGraph.write(" " + val + " " + Integer.toString(cntEdge));
                bufferedWriterEdge.write(Integer.toString(cntEdge) + " " + Integer.toString(getEdgeType(key, val)) + "\n");
                cntEdge += 1;
            }
            bufferedWriterGraph.write("\n");
        }
        bufferedWriterEdge.close();
        bufferedWriterWeight.close();
        bufferedWriterVertex.close();
        bufferedWriterGraph.close();
        System.out.println("all topic > 10 is : " + allCnt);
        System.out.println("all non-weight paper  is : " + cntV);
    }

    private Set<Integer> getTopKSet(int k, Set<Integer> oriSet, List<Integer> conditionSet) {
        Set<Integer> topKSet = new HashSet<>();

        for (int i = conditionSet.size() - 1; i >= 0; i--) {
            if (oriSet.contains(conditionSet.get(i))) {
                topKSet.add(conditionSet.get(i));
            }
            if (topKSet.size() == k) {
                return topKSet;
            }
        }

        return topKSet;
    }

    private void createNewAllEdge() {
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(rootPath + "/" + "alledges.txt"));
            BufferedWriter bufferedWriterAllEdges = new BufferedWriter(new FileWriter(dst_root + "/" + "alledges.txt"));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String s[] = line.split("\\s+");
                int leftId = Integer.parseInt(s[0]);
                int rightId = Integer.parseInt(s[1]);
                int rightType = getVertexType(rightId);
                if (rightType == 0) {
                    System.out.println("sdsdsdsdsd");
                }
                if (rightType == -1) {
                    continue;
                }
                if (!Paper2Year.containsKey(leftId)) {
                    pnbMap.put(leftId, new HashSet<>());
                    continue;
                }
                if (rightType == 2) {
                    int year = Paper2Year.get(leftId);
                    int oldVenuId = rightId - 811494;
                    rightId = venueyear2newID.get(oldVenuId).get(year) + 811494;
                } else if (rightType == 3) {
                    int oldFosId = rightId - 811637;
                    rightId = oldFosId + 815020;
                }
                if (pnbMap.containsKey(leftId)) {
                    pnbMap.get(leftId).add(rightId);
                } else {
                    Set<Integer> tmpSet = new HashSet<>();
                    tmpSet.add(rightId);
                    pnbMap.put(leftId, tmpSet);
                }
                if (pnbMap.containsKey(rightId)) {
                    pnbMap.get(rightId).add(leftId);
                } else {
                    Set<Integer> tmpSet = new HashSet<>();
                    tmpSet.add(leftId);
                    pnbMap.put(rightId, tmpSet);
                }
                bufferedWriterAllEdges.write(leftId + " " + rightId + "\n");
            }
            bufferedWriterAllEdges.close();
            // collect paper -> topic
            for (int i = 0; i < 409340; i++) {
                for (int key : pnbMap.get(i)) {
                    // topic
                    if (getNewVertexType(key) == 3) {
                        if (paper2topic.containsKey(i)) {
                            paper2topic.get(i).add(key);
                        } else {
                            Set<Integer> tmpSet = new HashSet<>();
                            tmpSet.add(key);
                            paper2topic.put(i, tmpSet);
                        }
                    }
                }
            }
            // link author -> paper -> topic
            // vertex >= 409340 && vertex < 811494
            for (int i = 409340; i < 811494; i++) {
                if (!pnbMap.containsKey(i)) {
                    continue;
                }
                for (int paper : pnbMap.get(i)) {
                    if (!paper2topic.containsKey(paper)) {
                        continue;
                    }
                    Set<Integer> topicSet = paper2topic.get(paper);
                    if (author2topic.containsKey(i)) {
                        Map<Integer, Integer> topicMap = author2topic.get(i);
                        for (int topic : topicSet) {
                            if (topicMap.containsKey(topic)) {
                                int count = topicMap.get(topic);
                                topicMap.replace(topic, count + 1);
                            } else {
                                topicMap.put(topic, 1);
                            }
                        }
                    } else {
                        Map<Integer, Integer> topicMap = new HashMap<>();
                        if (topicSet == null) {
                            System.out.println("sdsdsdsds");
                        }
                        for (int topic : topicSet) {
                            if (topicMap.containsKey(topic)) {
                                int count = topicMap.get(topic);
                                topicMap.replace(topic, count + 1);
                            } else {
                                topicMap.put(topic, 1);
                            }
                        }
                        author2topic.put(i, topicMap);
                    }
                }
            }
            System.out.println("build author -> topic over");
            for (int author : author2topic.keySet()) {
                List<Integer> sortList = new ArrayList<>();
                sortList.addAll(author2topic.get(author).keySet());
                sortList.sort(new tpoicComparator(author2topic.get(author)));
                authorSortedTopic.put(author, sortList);
            }
            // collect the topic -> paper  and sort
            for (int i = 815020; i < 881039; i++) {
                if (!pnbMap.containsKey(i)) {
                    continue;
                }
                if (nonTopic.contains(i - 815020)) {
                    System.out.println("this topic link paper is :  " + pnbMap.get(i).size());
                }
                sortTopic.add(i);
                topic2paper.put(i, pnbMap.get(i).size());
            }
//////            // compute sub-graph
            for (int i = 0; i < 409340 * 0.15; i++) {
                Random random = new Random();
                int id = random.nextInt(409340);

                while (deleteSet.contains(id)) {
                    id = random.nextInt(409340);
                }
                deleteSet.add(id);
                Set<Integer> papersNb = pnbMap.get(id);
                if (papersNb != null) {
                    for (int papernb : papersNb) {
                        pnbMap.get(papernb).remove(id);
                    }
                }
            }
//            // delete 20 % author
            for (int i = 0; i < 402154 * 0.15; i++) {
                Random random = new Random();
                int id = random.nextInt(402154) + 409340;

                while (deleteSet.contains(id)) {
                    id = random.nextInt(402154) + 409340;
                }
                Set<Integer> authorNb = pnbMap.get(id);
                if (authorNb != null) {
                    for (int aunb : authorNb) {
                        pnbMap.get(aunb).remove(id);
                    }
                }
                deleteSet.add(id);
            }
            deleteSet.add(815042);
            Set<Integer> authorNb = pnbMap.get(815042);
            if (authorNb != null) {
                for (int aunb : authorNb) {
                    pnbMap.get(aunb).remove(815042);
                }
            }
////            // delete 20% topic
//            for (int i = 0; i < (int) 66019 * 0.1; i++) {
//                Random random = new Random();
//                int id = random.nextInt(66019) + 815020;
//                while (deleteSet.contains(id)) {
//                    id = random.nextInt(66019) + 815020;
//                }
//                deleteSet.add(id);
//            }
//
//            // delete 20 % venue
//            for (int i = 0; i < 3256 * 0.1 ; i++) {
//                Random random = new Random();
//                int id = random.nextInt(3256) + 811494;
//                while (deleteSet.contains(id)) {
//                    id = random.nextInt(3256) + 811494;
//                }
//                deleteSet.add(id);
//            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}