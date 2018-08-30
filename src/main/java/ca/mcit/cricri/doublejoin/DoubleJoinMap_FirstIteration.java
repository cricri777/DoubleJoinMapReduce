package ca.mcit.cricri.doublejoin;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

@Slf4j
public class DoubleJoinMap_FirstIteration extends Mapper<LongWritable, Text, Text, Text> {

    private final String R_TO_S_MAPCODE = "R2S";
    private final String S_TO_T_MAPCODE = "S2T";
    private final String SEPARATOR = ",";

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // https://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        log.trace("incoming mapping from filename:[{}], key: [{}], value: [{}]", fileName, key, value.toString());

        String record = value.toString();
        String[] fields = record.split(SEPARATOR);

        String value1 = fields[0];
        String value2 = fields[1];

        Text keyOut = null;
        Text valueOut = null;
        switch (fileName) {
            case "R":
                keyOut = new Text(R_TO_S_MAPCODE + SEPARATOR + value2.toUpperCase());
                valueOut = new Text(fileName + SEPARATOR + record);
                context.write(keyOut, valueOut);
                break;
            case "S":
                Text keyOut1 = new Text(R_TO_S_MAPCODE + SEPARATOR + value1.toUpperCase());
                Text keyOut2 = new Text(S_TO_T_MAPCODE + SEPARATOR + value2.toUpperCase());
                Text valueOut1 = new Text(fileName + SEPARATOR + record);
                Text valueOut2 = new Text(fileName + SEPARATOR + record);
                context.write(keyOut1, valueOut1);
                context.write(keyOut2, valueOut2);

                break;
            case "T":
                keyOut = new Text(S_TO_T_MAPCODE + SEPARATOR + value2.toUpperCase());
                valueOut = new Text(fileName + SEPARATOR + record);
                context.write(keyOut, valueOut);
                break;
            default:
                log.info("hadoop file name does not follow convetion, should be R, S or T but was {}", fileName);
        }
    }

}