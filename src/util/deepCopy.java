package util;

import build.Edge;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class deepCopy {

    public deepCopy() {
    }

    public <E, T> Map<E, T> copy(Map<E, T> src) {
        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(src);

            ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
            ObjectInputStream in = new ObjectInputStream(byteIn);
            @SuppressWarnings("unchecked")
            Map<E, T> dest = (Map<E, T>) in.readObject();
            return dest;
        } catch (Exception e) {
            e.printStackTrace();
            return new HashMap<E, T>();
        }
    }

    public <E> Set<E> copy(Set<E> src) {
        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(src);

            ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
            ObjectInputStream in = new ObjectInputStream(byteIn);
            @SuppressWarnings("unchecked")
            Set<E> dest = (Set<E>) in.readObject();
            return dest;
        } catch (Exception e) {
            e.printStackTrace();
            return new HashSet<E>();
        }
    }

    public <E> ArrayList<E> copy(ArrayList<E> src) {
        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(src);

            ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
            ObjectInputStream in = new ObjectInputStream(byteIn);
            @SuppressWarnings("unchecked")
            ArrayList<E> dest = (ArrayList<E>) in.readObject();
            return dest;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<E>();
        }
    }

    public <E> List<E> copy(List<E> src) {
        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(src);

            ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
            ObjectInputStream in = new ObjectInputStream(byteIn);
            @SuppressWarnings("unchecked")
            List<E> dest = (List<E>) in.readObject();
            return dest;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<E>();
        }
    }

    public Map<Integer, Set<Integer>> copyMap(Map<Integer, Set<Integer>> src) {
        try {
            Set<Integer> keySet = src.keySet();
            Map<Integer, Set<Integer>> dest = new HashMap<Integer, Set<Integer>>();
            for (int key : keySet) {
                Set<Integer> valSet = src.get(key);
                Set<Integer> val = new HashSet<Integer>(valSet);
                dest.put(key, val);
            }
            return dest;
        } catch (Exception e) {
            e.printStackTrace();
            return new HashMap<Integer, Set<Integer>>();
        }
    }

    public Map<Edge, Integer> copyAsso(Map<Edge, Integer> src) {
        try {
            Set<Edge> keySet = src.keySet();
            Map<Edge, Integer> dest = new HashMap<Edge, Integer>();
            for (Edge key : keySet) {
                dest.put(key, new Integer(src.get(key)));
            }
            return dest;
        } catch (Exception e) {
            e.printStackTrace();
            return new HashMap<Edge, Integer>();
        }
    }


}
