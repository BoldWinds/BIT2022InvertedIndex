import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InvertedIndex {

    private static class InvertedIndexMapper
            extends Mapper<LongWritable,Text,Text,Text>{

        String fileName = null;
        Text mapKeyOut = new Text();
        Text mapValueOut = new Text();

        // 在setup中通过切片来获取文件名
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            FileSplit files = (FileSplit) context.getInputSplit();
            fileName = files.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            //将一行数据以空格位分隔符读入datas
            String[] datas = value.toString().split(" ");
            //索引号即是第一个
            String index = datas[0];
            //map：key为单词value为出现次数
            Map<String, Integer> map = new HashMap<String, Integer>();
            //更新单词出现次数
            for (int i = 1; i < datas.length; i++) {
                String word = datas[i];
                if (map.containsKey(word))
                    map.put(word, map.get(word) + 1);
                else
                    map.put(word, 1);
            }

            //keySet是单词的集合
            Set<String> keySet = map.keySet();
            for (String mapKey : keySet) {
                // mapKeyOut设置为key值
                mapKeyOut.set(mapKey);
                // 按要求拼接
                mapValueOut.set(fileName+":"+index);
                // 每一次循环都要写一次
                context.write(mapKeyOut, mapValueOut);
            }

        }

    }

    private static class InvertedIndexReducer
            extends TableReducer<Text, Text, ImmutableBytesWritable> {

        Text reduceValueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 用StringBuilder来进行拼接
            StringBuilder stringBuilder = new StringBuilder();
            for (Text v : values) {
                stringBuilder.append(v.toString()).append(";");
            }
            // 去掉最后一行的分号
            reduceValueOut.set(stringBuilder.substring(0, stringBuilder.length() - 1));

            //设置word为Row Key 构建 Put对象
            Put put = new Put(key.toString().getBytes());
            //指定插入到哪个列族，插入的列名和值
            put.addColumn("col_family".getBytes(), "info".getBytes(), reduceValueOut.toString().getBytes());

            // 写一次即可
            context.write(null,put);
        }

    }



    public static void main(String[] args) throws Exception {
        //获取conf
        Configuration conf = new Configuration();
        conf=HBaseConfiguration.create(conf);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //创建job对象
        Job job = Job.getInstance(conf, "InvertedIndex");
        //指定Jar包主类
        job.setJarByClass(InvertedIndex.class);
        //设置mapper类
        job.setMapperClass(InvertedIndexMapper.class);
        //设置Combiner类，一般设定为Reducer类
        job.setCombinerClass(InvertedIndexReducer.class);
        //设置Reducer类
        job.setReducerClass(InvertedIndexReducer.class);
        //设置输出key类型
        job.setOutputKeyClass(Text.class);
        //设置输出value类型
        job.setOutputValueClass(Text.class);
        //写表
        TableMapReduceUtil.initTableReducerJob("test_table", MyReducer.class, job);
        //设置文件输入路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //设置文件输出路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //提交作业
        job.waitForCompletion(true);
    }
}





