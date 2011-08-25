package pb.fantahadoop;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class Main {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, MyWritable> {

        private static boolean isHeader(String line) {
            return line.indexOf("\"Autogol\"") != -1;
        }

        private static boolean isSenzaVoto(String line) {
            return line.indexOf("\"s.v.\"") != -1 || line.indexOf("\"-\"") != -1;
        }

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, MyWritable> output, Reporter reporter) throws IOException {
            String rowText = value.toString();
            if (!isHeader(rowText) && !isSenzaVoto(rowText)) {
                CsvRow row = new CsvRow(rowText);
                output.collect(new Text(String.format("%s,%s", row.nome, row.ruolo)), new MyWritable(1, row.calcolaVoto()));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, MyWritable, Text, MyWritable> {

        @Override
        public void reduce(Text key, Iterator<MyWritable> iterator, OutputCollector<Text, MyWritable> output, Reporter reporter) throws IOException {
            MyWritable val = new MyWritable();
            
            MyWritable o;
            while (iterator.hasNext()) {
                o = iterator.next();
                val.presenze += o.presenze;
                val.sommaVoti += o.sommaVoti;
            }
            output.collect(key, val);
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("FantaHadoop");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MyWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(MyOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
