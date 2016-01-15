/**
 * Created by Silun Wang on 11/20/15.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LangModel {

    static int limit = 5;

    // HBase configs
    static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "ec2-54-175-26-199.compute-1.amazonaws.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "ec2-54-175-26-199.compute-1.amazonaws.com:60000");
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text gram = new Text();
        private Text postTerm_cnt = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("\t");
            String cnt = arr[1];
            // <phrase, |cnt>
            postTerm_cnt.set("0-" + cnt);
            gram.set(arr[0]);
            context.write(gram, postTerm_cnt);
            String[] strArr = arr[0].split(" +");
            if (strArr.length > 1) {
                // <phrase, word|cnt>
                gram.set(StringUtils.join(Arrays.copyOfRange(strArr, 0, strArr.length - 1), " "));
                postTerm_cnt.set(strArr[strArr.length - 1] + "-" + cnt);
                context.write(gram, postTerm_cnt);
            }
        }
    }

    public static class IntSumReducer
            extends TableReducer<Text, Text, Text> {

        class pair {
            String term;
            int count;
            public pair(String s, int c) {
                this.term = s;
                this.count = c;
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            ArrayList<pair> list = new ArrayList();
            int sum = 10000000;
            for (Text val : values) {
                System.out.println("val:" + val);
                String[] arr = val.toString().split("-");
                // <phrase, null, cnt>
                if (arr[0].equals("0"))
                    sum = Integer.parseInt(arr[1]);
                // <phrase, word, cnt>
                else {
                    String word = arr[0];
                    int count = Integer.parseInt(arr[1]);
                    list.add(new pair(word, count));
                }
            }
            Collections.sort(list, new Comparator<pair>() {
                @Override
                public int compare(pair pair, pair t1) {
                    if (pair.count == t1.count)
                        return pair.term.compareTo(t1.term);
                    return t1.count - pair.count;
                }
            });
            int k = 0;
            while (k < limit && k < list.size()) {
                Put put = new Put(Bytes.toBytes(key.toString()));
                double prob = ((double) list.get(k).count) / sum;
                put.add(Bytes.toBytes("cf"), Bytes.toBytes(list.get(k).term), Bytes.toBytes(String.valueOf(Math.round(prob * 100.0) / 100.0)));
                context.write(key, put);
                k ++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(conf, "language model");
        job.setJarByClass(LangModel.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Put.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        TableMapReduceUtil.initTableReducerJob(
                "model",                  // output table
                IntSumReducer.class,      // reducer class
                job);
        //job.setOutputKeyClass(ImmutableBytesWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        limit = Integer.parseInt(args[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}