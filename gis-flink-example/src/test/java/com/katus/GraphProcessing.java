package com.katus;

import cn.edu.zju.gis.td.common.collection.Tuple;
import cn.edu.zju.gis.td.common.io.FsManipulator;
import cn.edu.zju.gis.td.common.io.FsManipulatorFactory;
import cn.edu.zju.gis.td.common.io.LineIterator;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 图结构处理工作
 *
 * @author SUN Katus
 * @version 1.0, 2022-11-28
 */
@Slf4j
public class GraphProcessing {
    public static void main(String[] args) throws IOException {
//        log.info("{}", checkNumber("F:\\data\\graduation\\graph\\center_points.csv", true, 16, 17));
//        log.info("{}", checkNumber("F:\\data\\graduation\\graph\\nodes_real.csv", true, 2, 3));
//        log.info("{}", checkMulti("F:\\data\\graduation\\graph\\edges_ori_f.csv", true));
        generate(new String[]{
                "F:\\data\\graduation\\graph\\edges_f_ori.csv",
                "F:\\data\\graduation\\graph\\cp_f.csv",
                "F:\\data\\graduation\\graph\\real_nodes_f.csv",
                "F:\\data\\graduation\\graph\\edges_f_pair.tsv",
                "F:\\data\\graduation\\graph\\nodes_f.tsv"
        }, true, true);
    }

    /**
     * 检查是否有重复的node
     */
    private static boolean checkNumber(String filename, boolean hasTitle, int c1, int c2) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator(filename);
        if (hasTitle) {
            it.next();
        }
        int lineNum = 0, temp;
        Set<Tuple<Double, Double>> set = new HashSet<>();
        while (it.hasNext()) {
            String line = it.next();
            String[] items = line.split("\t");
            temp = set.size();
            set.add(new Tuple<>(Double.parseDouble(items[c1]), Double.parseDouble(items[c2])));
            if (temp + 1 != set.size()) {
                log.info("({}, {})", items[c1], items[c2]);
            }
            lineNum++;
        }
        log.info("line: {}, distinct: {}", lineNum, set.size());
        return lineNum == set.size();
    }

    /**
     * 检查是否存在多段线
     */
    private static boolean checkMulti(String filename, boolean hasTitle) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        LineIterator it = fsManipulator.getLineIterator(filename);
        if (hasTitle) {
            it.next();
        }
        boolean flag = true;
        while (it.hasNext()) {
            String line = it.next();
            String wkt = line.split("\t")[0];
            if (wkt.substring(19, wkt.length() - 3).contains("(")) {
                log.info("{}", wkt);
                flag = false;
            }
        }
        return flag;
    }

    /**
     * 生成图的节点与边
     */
    private static void generate(String[] filenames, boolean hasTitle, boolean needPair) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create();
        // 读取点判断是否存在重合点
        LineIterator it = fsManipulator.getLineIterator(filenames[1]);
        List<Node> nodeList = new ArrayList<>();
        if (hasTitle) it.next();
        while (it.hasNext()) {
            nodeList.add(Node.loadFromCenterPoints(it.next()));
        }
        long count = nodeList.size();
        it = fsManipulator.getLineIterator(filenames[2]);
        if (hasTitle) it.next();
        while (it.hasNext()) {
            nodeList.add(Node.loadFromNodes(it.next(), count));
        }
        Map<Tuple<Double, Double>, Node> map = new HashMap<>();
        for (Node node : nodeList) {
            Tuple<Double, Double> key = new Tuple<>(node.x, node.y);
            if (map.containsKey(key)) {
                Node oriNode = map.get(key);
                oriNode.setNode(true);
                log.info("Duplicate: ({}, {}) id: {}, deleteId: {}", node.x, node.y, oriNode.getId(), node.getId());
            } else {
                map.put(key, node);
            }
        }
        log.info("{}, {}", nodeList.size(), map.size());
        // 生成点文件内容
        List<String> contents = new ArrayList<>();
        contents.add(Node.title());
        for (Map.Entry<Tuple<Double, Double>, Node> entry : map.entrySet()) {
            contents.add(entry.getValue().toLine());
        }
        fsManipulator.writeTextToFile(filenames[4], contents);
        contents.clear();
        // 读入线并获取起点和终点的节点号
        it = fsManipulator.getLineIterator(filenames[0]);
        List<Edge> edgeList = new ArrayList<>();
        if (hasTitle) it.next();
        while (it.hasNext()) {
            edgeList.add(Edge.loadFromEdges(it.next()));
        }
        // 计算所有边的起点与终点
        for (Edge edge : edgeList) {
            if (map.containsKey(edge.getStartPoint())) {
                edge.setStartId(map.get(edge.getStartPoint()).id);
            } else {
                Node node = findClosestNode(map, edge.getStartPoint());
                edge.setStartId(node.getId());
                log.info("{} ({}, {})", edge.getStartPoint(), node.x, node.y);
            }
            if (map.containsKey(edge.getEndPoint())) {
                edge.setEndId(map.get(edge.getEndPoint()).id);
            } else {
                Node node = findClosestNode(map, edge.getEndPoint());
                edge.setEndId(node.getId());
                log.info("{} ({}, {})", edge.getEndPoint(), node.x, node.y);
            }
        }
        // 生成边文件内容
        contents.add(Edge.title());
        for (Edge edge : edgeList) {
            contents.add(edge.toLine());
        }
        // 是否针对双向道路生成对向边对
        if (needPair) {
            for (Edge edge : edgeList) {
                if (!edge.isOneway()) {
                    Edge pair = edge.pair(contents.size());
                    contents.add(pair.toLine());
                }
            }
        }
        fsManipulator.writeTextToFile(filenames[3], contents);
        contents.clear();
    }

    /**
     * 在map中获取最近的点
     */
    private static Node findClosestNode(Map<Tuple<Double, Double>, Node> map, Tuple<Double, Double> point) {
        double dis2 = Integer.MAX_VALUE;
        Tuple<Double, Double> closestPoint = null;
        for (Map.Entry<Tuple<Double, Double>, Node> entry : map.entrySet()) {
            Tuple<Double, Double> p = entry.getKey();
            if (Math.abs(p._1() - point._1()) < 1 && Math.abs(p._2() - point._2()) < 1) {
                double v = Math.pow(p._1() - point._1(), 2) + Math.pow(p._2() - point._2(), 2);
                if (dis2 > v) {
                    dis2 = v;
                    closestPoint = p;
                }
            }
        }
        if (!map.containsKey(closestPoint)) {
            log.error("Can not find closest point!");
            return null;
        }
        return map.get(closestPoint);
    }

    @Getter
    @Setter
    public static class Node {
        private long id;
        private boolean isNode, isCenter;
        private String wkt;
        private double length, x, y;

        public static Node loadFromCenterPoints(String line) {
            String[] items = line.split("\t");
            Node node = new Node();
            node.setId(Long.parseLong(items[1]));
            node.setNode(false);
            node.setCenter(true);
            node.setWkt(items[0].substring(1, items[0].length() - 1));
            node.setLength(Double.parseDouble(items[14]));
            node.setX(Double.parseDouble(items[15]));
            node.setY(Double.parseDouble(items[16]));
            return node;
        }

        public static Node loadFromNodes(String line, long centerPointNumber) {
            String[] items = line.split("\t");
            Node node = new Node();
            node.setId(Long.parseLong(items[1]) + centerPointNumber);
            node.setNode(true);
            node.setCenter(false);
            node.setWkt(items[0].substring(1, items[0].length() - 1));
            node.setX(Double.parseDouble(items[2]));
            node.setY(Double.parseDouble(items[3]));
            return node;
        }

        public static String title() {
            return "id\tisNode\tisCenter\tlength\tx\ty\twkt";
        }

        public String toLine() {
            return String.valueOf(id) + '\t' + isNode + '\t' + isCenter + '\t' + length + '\t' + x + '\t' + y + '\t' + wkt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return Double.compare(node.x, x) == 0 && Double.compare(node.y, y) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(x, y);
        }
    }

    @Data
    public static class Edge {
        private long id;
        private long startId, endId;
        private String wkt;
        private String osmId;
        private String fClass;
        private String oriName, oriRef, newName, newRef;
        private boolean isOneway;
        private double maxSpeed;
        private boolean isBridge, isTunnel;
        private int hierarchy;
        private double length;
        private Tuple<Double, Double> startPoint, endPoint;
        private List<Double> directions = new ArrayList<>();

        public String toLine() {
            return String.valueOf(id) + '\t' + startId + '\t' + endId + '\t' + osmId + '\t' +
                    fClass + '\t' + oriName + '\t' + oriRef + '\t' +
                    newName + '\t' + newRef + '\t' + isOneway + '\t' +
                    maxSpeed + '\t' + isBridge + '\t' + isTunnel + '\t' +
                    hierarchy + '\t' + length + '\t' + String.format("{%s}", directions.stream().map(Object::toString).collect(Collectors.joining(","))) + '\t' + wkt;
        }

        public Edge pair(long id) {
            Edge pairEdge = new Edge();
            pairEdge.setId(id);
            pairEdge.setStartId(endId);
            pairEdge.setEndId(startId);
            pairEdge.setWkt(wkt);
            pairEdge.setOsmId(osmId);
            pairEdge.setFClass(fClass);
            pairEdge.setOriName(oriName);
            pairEdge.setOriRef(oriRef);
            pairEdge.setNewName(newName);
            pairEdge.setNewRef(newRef);
            pairEdge.setOneway(isOneway);
            pairEdge.setMaxSpeed(maxSpeed);
            pairEdge.setBridge(isBridge);
            pairEdge.setTunnel(isTunnel);
            pairEdge.setHierarchy(hierarchy);
            pairEdge.setLength(length);
            pairEdge.setWkt(wkt.substring(12, wkt.length() - 1));
            refactorWkt(pairEdge, "T");
            return pairEdge;
        }

        public static Edge loadFromEdges(String line) {
            String[] items = line.split("\t");
            Edge edge = new Edge();
            edge.setId(Long.parseLong(items[1]));
            edge.setWkt(items[0].substring(19, items[0].length() - 3));
            edge.setOsmId(items[2]);
            edge.setFClass(items[4]);
            edge.setOriName(items[5]);
            edge.setOriRef(items[6]);
            edge.setNewName(items[7]);
            edge.setNewRef(items[8]);
            edge.setMaxSpeed(Double.parseDouble(items[10]));
            edge.setBridge(items[11].equals("T"));
            edge.setTunnel(items[12].equals("T"));
            edge.setHierarchy(Integer.parseInt(items[13]));
            edge.setLength(Double.parseDouble(items[14]));
            edge.setOneway(!"B".equals(items[9]));
            refactorWkt(edge, items[9]);
            return edge;
        }

        public static String title() {
            return "id\tstartId\tendId\tosmId\tfClass\toriName\toriRef\tnewName\tnewRef\tisOneway\tmaxSpeed\tisBridge\tisTunnel\thierarchy\tlength\tdirections\twkt";
        }

        private static void refactorWkt(Edge edge, String oneway) {
            List<String> list = new ArrayList<>();
            String[] cords = edge.getWkt().split(",");
            if (oneway.equals("T")) {
                for (int i = cords.length - 1; i >= 0; i--) {
                    list.add(cords[i]);
                }
            } else {
                list.addAll(Arrays.asList(cords));
            }
            edge.setWkt(String.format("LINESTRING (%s)", String.join(",", list)));
            List<Tuple<Double, Double>> points = parseFromPointCords(list);
            edge.setStartPoint(points.get(0));
            edge.setEndPoint(points.get(points.size() - 1));
            for (int i = 0; i < points.size() - 1; i++) {
                edge.getDirections().add(calDirect(points.get(i), points.get(i + 1)));
            }
        }

        private static List<Tuple<Double, Double>> parseFromPointCords(List<String> cords) {
            return cords.stream().map(Edge::parseFromPointCord).collect(Collectors.toList());
        }

        private static Tuple<Double, Double> parseFromPointCord(String cord) {
            String[] ps = cord.split(" ");
            return new Tuple<>(Double.valueOf(ps[0]), Double.valueOf(ps[1]));
        }

        private static double calDirect(Tuple<Double, Double> startPoint, Tuple<Double, Double> endPoint) {
            double deltaX = endPoint._1() - startPoint._1(), deltaY = endPoint._2() - startPoint._2();
            if (deltaX == 0.0) {
                return deltaY >= 0 ? 0.0 : 180.0;   // 如果位置没有变化则方向角默认值为0.0
            }
            if (deltaY == 0.0) {
                return deltaX > 0 ? 90.0 : 270.0;
            }
            double red = Math.atan(deltaX / deltaY);
            if (deltaX > 0) {
                return deltaY > 0 ? red2Deg(red) : red2Deg(red + Math.PI);
            } else {
                return deltaY > 0 ? red2Deg(red + 2 * Math.PI) : red2Deg(red + Math.PI);
            }
        }

        private static double red2Deg(double red) {
            return red / Math.PI * 180.0;
        }
    }
}
