package pb.fantahadoop;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class MyOutputFormat<K, V extends MyWritable> extends FileOutputFormat {

    protected class SimpleRecordWriter implements RecordWriter<K, V> {

        private DataOutputStream out;

        public SimpleRecordWriter(DataOutputStream out) throws IOException {
            this.out = out;
        }

        @Override
        public synchronized void write(K key, V value) throws IOException {
            StringBuilder str = new StringBuilder();
            str.append(key.toString());
            str.append(",");
            str.append(value.sommaVoti);
            str.append(",");
            str.append(value.presenze);
            str.append(",");
            str.append(value.sommaVoti / value.presenze);
            str.append("\n");
            out.write(str.toString().getBytes("utf-8"));
        }

        @Override
        public synchronized void close(Reporter reporter) throws IOException {
            out.close();
        }
    }

    @Override
    public RecordWriter getRecordWriter(FileSystem ignored, JobConf conf, String name, Progressable progress) throws IOException {
        Path file = FileOutputFormat.getTaskOutputPath(conf, name);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream out = fs.create(file, progress);
        return new SimpleRecordWriter(out);
    }
}
