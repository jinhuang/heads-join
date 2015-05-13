package com.iojin.heads.mr;

import com.iojin.heads.single.Join;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Dummy extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new Dummy(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf());
        job.getConfiguration().set("yarn.application.classpath", "$HADOOP_HOME/etc/hadoop," +
                "$HADOOP_HOME/share/hadoop/common/*," +
                "$HADOOP_HOME/share/hadoop/common/lib/*," +
                "$HADOOP_HOME/share/hadoop/mapreduce/*," +
                "$HADOOP_HOME/share/hadoop/mapreduce/lib/*," +
                "$HADOOP_HOME/share/hadoop/hdfs/*," +
                "$HADOOP_HOME/share/hadoop/hdfs/lib/*," +
                "$HADOOP_HOME/share/hadoop/yarn/*," +
                "$HADOOP_HOME/share/hadoop/yarn/lib/*," +
                "$HAMA_HOME/*," +
                "$HAMA_HOME/lib/*");
        job.setJarByClass(Dummy.class);
        job.setMapperClass(DummyMapper.class);
        job.setReducerClass(DummyReducer.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Join.run(args);
        return  0;
//        if (job.waitForCompletion(true)) {
//            return 0;
//        } else return -1;
    }

    public static class DummyMapper
            extends Mapper<Object, Text, LongWritable, LongWritable> {

        public DummyMapper() {}

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(new LongWritable(0), new LongWritable(1));
        }
    }

    public static class DummyReducer
            extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

        public DummyReducer(){}

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values,
                              Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}
