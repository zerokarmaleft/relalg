import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

public class NaturalJoinRMapperTest {

    @Test public void processValidRecord()
        throws IOException, InterruptedException {

        Text key = new Text("0");
        Text value = new Text("A");

        new MapDriver<Text, Text, Text, TextDuple>()
            .withMapper(new NaturalJoinRMapper())
            .withInput(key, value)
            .withOutput(value, new TextDuple(new Text("R"), key))
            .runTest();
    }
}
