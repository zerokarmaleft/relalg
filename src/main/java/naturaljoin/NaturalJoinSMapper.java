import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NaturalJoinSMapper
    extends Mapper<Text, Text, Text, TextDuple> {

    @Override protected void map(Text key,
                                 Text value,
                                 Context context)
        throws IOException, InterruptedException {

        System.err.printf("S: (%s => %s)", key, value);
        // key-value pair of (key, ("S", value)) is used to denote
        // (b, (S, c)) where S is a relation
        context.write(key,
                      new TextDuple(new Text("S"), value));
    }
}
