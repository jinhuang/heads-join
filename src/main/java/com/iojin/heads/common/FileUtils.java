package com.iojin.heads.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by Jin on 3/9/15.
 */
public class FileUtils {

    public static double[] readDoubleArray(Configuration conf, String path) {
        String line = readSingleLine(conf, path);
        return FormatUtils.toDoubleArray(line);
    }

    public static String readSingleLine(Configuration conf, String path) {
        Path pt = new Path(path);
        String line = "0.0";
        try {
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            line = br.readLine();
            br.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
        return line;
    }

    public static List<Double> getMultiData(String addr,int size){
        List<Double> data = new ArrayList<Double>();
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(addr));
            String line = br.readLine();
            while(null != line) {
                String[] eles = line.split("\\s");
                for (int j = 0; j < size; j++) {
                    data.add(Double.valueOf(eles[j]));
                }
                line = br.readLine();
            }
            br.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return data;
    }

    public static double[] getMultiFromCache(Mapper.Context context,
                                             String name,
                                             int lineLength)
            throws IOException {
        List<Double> list = new ArrayList<Double>();
        int counter = 0;
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            File file = new File("./" + name);
            if (!file.isDirectory()) {
                counter++;
               list.addAll(getMultiData(file.getPath(), lineLength));
            }
        }
        double[] array = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    public static void addToCache(
            Job job, String outputPath, String name)
            throws IOException, URISyntaxException {

        Path path = new Path(outputPath);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FileStatus status[] = fileSystem.listStatus(path);
        int counter = 0;
        for (FileStatus file : status) {
            if (file.getPath().toString().contains("part-r")) {
                String namePath = file.getPath() + "#" + name;
                counter ++;
                job.addCacheFile(new URI(namePath));
            }
        }
    }

    public static double [] getData(String addr,int size){
        double [] datas = new double[size];
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(addr));
            String[] tokens = br.readLine().split("\\s");
            for (int i = 0; i < size; i++) {
                tokens[i] = tokens[i].trim();
                if (tokens[i].length() > 0) {
                    datas[i] = Double.valueOf(tokens[i]);
                }
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return datas;
    }

    public static String getNameFromPath(String path) {
        return path.substring(path.lastIndexOf("/")+1, path.length());
    }

    public static double[] getFromDistributedCache(Configuration conf, String name, int length) throws IOException {
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
        double[] vector = new double[length];
        for(Path path:localFiles){
            if(path.toString().indexOf(name) > -1){
                vector = getData(path.toString(), vector.length);
            }
        }
        return vector;
    }

    public static void deleteIfExistOnHDFS(Configuration conf, String path) {
        // commented out for s3 operation 0705
        Path pt = new Path(path);
        try {
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(pt)) {
                fs.delete(pt, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return;
    }

    public static List<Double[]> readMultiFromHDFS(Configuration conf, String path) {
        Path pt = new Path(path);
        List<Double[]> arrayResult = new ArrayList<Double[]>();
        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus status[] = fs.listStatus(pt);
            for (FileStatus file : status) {
//				System.out.println("read from " + file.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                String line = br.readLine();
                while(null != line) {
                    Double[] each = FormatUtils.toObjectDoubleArray(line);
//					if (HistUtil.sum(FormatUtil.toDoubleArray(each)) != 0.0d) {
                    arrayResult.add(each);
//					}
                    line = br.readLine();
                }
                br.close();
            }
            //System.out.println(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arrayResult;
    }

//    public static List<Double[]> readMultiFromDistributedCache(Configuration conf, String name) throws IOException {
//        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
//        List<Double[]> list = new ArrayList<Double[]>();
//        for(Path path:localFiles){
//            if(path.toString().indexOf(name) > -1){
//                File file = new File(path.toString());
//                if (!file.isDirectory()) {
//                    BufferedReader br = new BufferedReader(new FileReader(file));
//                    String line = br.readLine();
//                    while(null != line) {
//                        Double[] each = FormatUtils.toObjectDoubleArray(line);
////						if (HistUtil.sum(FormatUtil.toDoubleArray(each)) != 0.0d) {
//                        list.add(each);
////						}
//                        line = br.readLine();
//                    }
//                    br.close();
//                }
//            }
//        }
//        return list;
//    }

    public static String readSingleFromHDFS(Configuration conf, String path) {
        Path pt = new Path(path);
        String line = "0.0";
        try {
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            line = br.readLine();
            br.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
        return line;
    }

    public static List<Double[]> readArraysLocalFile(String path) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
        List<Double[]> result = new ArrayList<Double[]>();
        String line = "";
        while((line = br.readLine()) != null) {
            result.add(FormatUtils.toObjectDoubleArray(FormatUtils.toDoubleArray(line)));
        }
        br.close();
        return result;
    }
}
