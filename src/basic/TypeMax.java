package basic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import build.Edge;
import util.MetaPath;
import util.deepCopy;
import build.BatchSearch;

public class TypeMax {
    private int vertexType[] = null;//vertex -> type
    private double weight[] = null;
    private int queryK = -1;
    private MetaPath queryMPath = null;
    private double constraints[] = null;
    Map<Integer, Set<Integer>> pnbMap, tmpMap = new HashMap<Integer, Set<Integer>>();
    Map<Edge, Integer> edgeAsso, tmpAsso = new HashMap<Edge, Integer>();
    Map<Integer, Set<Edge>> verAsso = new HashMap<Integer, Set<Edge>>();
    List<Integer> sortedType1, tmptype1, sortedType2, tmptype2 = new ArrayList<Integer>();

    public TypeMax(int vertexType[], double weight[], int queryK, MetaPath queryMPath, Map<Integer, Set<Integer>> pnbMap,
                   BatchSearch association, List<Integer> sortedType1, List<Integer> sortedType2) {
        this.vertexType = vertexType;
        this.weight = weight;
        this.queryK = queryK;
        this.queryMPath = queryMPath;
        this.pnbMap = pnbMap;
        this.edgeAsso = association.edgeAsso;
        this.verAsso = association.verAsso;
        this.sortedType1 = sortedType1;
        this.sortedType2 = sortedType2;
    }

    public double getTypeMax(double csts[], int type, Set<Integer> community) {
        constraints = csts;
        tmptype1 = new ArrayList<Integer>(sortedType1);
        tmptype2 = new ArrayList<Integer>(sortedType2);
        deepCopy deepCopy = new deepCopy();
        tmpMap = deepCopy.copyMap(pnbMap);
        tmpAsso = deepCopy.copyAsso(edgeAsso);


        int minVer = 0;
        double f = -1;
        //delete vertices whose weight is smaller (or equal to) the constraints
        int i = 0;
        int checkV = sortedType1.get(i);
        while (i < sortedType1.size()) {
            checkV = sortedType1.get(i);
            if (weight[checkV] <= constraints[0] && tmptype1.contains(checkV)) {
                DeleVer(checkV, tmptype1);
            }
            i++;
        }
        i = 0;
        checkV = sortedType2.get(i);
        while (i < sortedType2.size()) {
            checkV = sortedType2.get(i);
            if (weight[checkV] < constraints[1] && tmptype2.contains(checkV)) {
                DeleVer(checkV, tmptype2);
            }
            i++;
        }
        if (tmptype1.isEmpty()) {
            return -1;
        }

        while (!tmptype1.isEmpty()) {
            community.clear();
            community.addAll(tmptype1);
            if (type == queryMPath.getEndType()) {
                minVer = tmptype1.get(0);
                DeleVer(minVer, tmptype1);
            }
            if (type == queryMPath.getSecondType()) {
                if (tmptype2.isEmpty()) {
                    return f;
                }
                minVer = tmptype2.get(0);
                DeleVer(minVer, tmptype2);
            }
            f = Math.max(f, weight[minVer]);
        }
        tmpMap.forEach((key, value) -> {
            if (value.size() >= queryK) System.out.println(key + "    " + value);
        });
        return f;
    }

    public void DeleVer(int v, List<Integer> vertices) {
        if (vertices.isEmpty()) {
            return;
        }
        if (vertices.contains(v)) {
            vertices.remove(vertices.indexOf(v));
        } else {
            return;
        }
        if (vertexType[v] == queryMPath.getEndType()) {

            Set<Integer> pnbSet = tmpMap.get(v);
            for (int pnb : pnbSet) {
                if (tmptype1.contains(pnb)) {
                    Set<Integer> tmpSet = tmpMap.get(pnb);
                    tmpSet.remove(v);
                    if (tmpSet.size() < queryK) {
                        DeleVer(pnb, tmptype1);
                    } else {
                        tmpMap.replace(pnb, tmpSet);
                    }
                }
            }
        } else {
            if (vertexType[v] == queryMPath.getSecondType()) {

                Set<Edge> endptsSet = verAsso.get(v);
                for (Edge endpts : endptsSet) {
                    int start = endpts.getKey()[0];
                    int end = endpts.getKey()[1];
                    int count = tmpAsso.get(endpts);
                    count = count - 1;
                    if (count == 0) {
                        Set<Integer> nb = tmpMap.get(start);
                        nb.remove(end);
                        if (nb.size() < queryK && tmptype1.contains(start)) {
                            DeleVer(start, tmptype1);
                        } else {
                            if (nb.size() >= queryK) {
                                tmpMap.replace(start, nb);
                            }
                        }
                        nb = tmpMap.get(end);
                        nb.remove(start);
                        if (nb.size() < queryK && tmptype1.contains(end)) {
                            DeleVer(end, tmptype1);
                        } else {
                            if (nb.size() >= queryK) {
                                tmpMap.replace(end, nb);
                            }
                        }
                    }
                    tmpAsso.replace(endpts, count);
                }
            }
        }
    }
}