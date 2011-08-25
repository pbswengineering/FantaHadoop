package pb.fantahadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class MyWritable implements Writable {

    public int presenze;
    public double sommaVoti;

    public MyWritable() {
    }

    public MyWritable(int presenze, double sommaVoti) {
        this.presenze = presenze;
        this.sommaVoti = sommaVoti;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(presenze);
        out.writeDouble(sommaVoti);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        presenze = in.readInt();
        sommaVoti = in.readDouble();
    }
    
    @Override
    public String toString() {
        return "" + presenze + "," + sommaVoti;
    }
}
