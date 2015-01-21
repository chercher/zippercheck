package com.dianping.data;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 15-1-19
 * Time: 下午6:17
 * To change this template use File | Settings | File Templates.
 */
public class ZipperCheck extends Configured implements Tool {

    public static class ZipperCheckMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text id = new Text();
        private Text dt = new Text();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] fields = StringUtils.split(line);

            id.set(fields[0]);

            int size = fields.length;
            for (int i=1;i<size;i++) {
                dt.set(fields[i]);
                output.collect(id, dt);
            }
        }
    }

    public static class ZipperCheckReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        String first_dt;
        String last_dt;

        public void configure(JobConf job) {
            this.first_dt = job.get("first_dt");
            this.last_dt = job.get("last_dt");
        }
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            List<String> dt = new ArrayList<String>();
            Text errordt = new Text();
            boolean error = false;

            while (values.hasNext()) {
                dt.add(values.next().toString());
            }

            Collections.sort(dt, new Comparator<String>() {
                public int compare(String a, String b){
                    return a.compareTo(b);
                }
            });

            int size = dt.size();
            int check_size = (size - 2) / 2;

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

            try {
                boolean if_after = df.parse(dt.get(0)).after(df.parse(first_dt));
                if (size == 2) {
                    if (if_after || (!StringUtils.equals(dt.get(size-1), last_dt))) {
                        errordt.set(StringUtils.join(dt.toArray(), " "));
                        output.collect(key, errordt);
                    }

                } else {
                    if (if_after || (!StringUtils.equals(dt.get(size-1), last_dt))) {
                        System.out.println("1");
                        error = true;
                    }
                    for (int i = 0; i < check_size; i++) {
                        if (!StringUtils.equals(dt.get(2 * i + 1), dt.get(2 * i + 2))) {
                            System.out.println("2");
                            error = true;
                            break;
                        }
                    }
                    if (error) {
                        errordt.set(StringUtils.join(dt.toArray(), " "));
                        output.collect(key, errordt);
                    }
                }
            } catch (ParseException ex) {
                throw new RuntimeException("dt parse error" + ex.getMessage());
            }
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), ZipperCheck.class);
        conf.setJobName("zippercheck");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(ZipperCheckMap.class);
        conf.setReducerClass(ZipperCheckReduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.set("first_dt", args[0]);
        conf.set("last_dt", "3000-12-31");

        Path inputPath = new Path(args[1]);
        FileSystem fs = inputPath.getFileSystem(conf);
        long inputLength = fs.getContentSummary(inputPath).getLength();
        int reducers = Math.round(inputLength / 128 * 1024 * 1024);
        if (reducers > 200)
            reducers = 200;

        conf.set("mapred.reduce.tasks", Integer.toString(reducers));

        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        Job job = new Job(conf);
        int retCode = job.waitForCompletion(true) ? 0 : 1;
        return retCode;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ZipperCheck(), args);
        System.exit(exitCode);
    }
}
