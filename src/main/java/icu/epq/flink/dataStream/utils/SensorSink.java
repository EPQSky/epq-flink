package icu.epq.flink.dataStream.utils;

import icu.epq.scala.entity.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.io.FileOutputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class SensorSink extends RichSinkFunction<SensorReading> {

    private FileOutputStream out = null;
    private final File file = new File("D:/Workplace/sensor-reading.out");
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        out = new FileOutputStream(file, true);
    }

    @Override
    public void invoke(SensorReading value, Context context) throws Exception {
        String msg = LocalDateTime.now().format(DATE_FORMAT) + " " + value.toString() + "\n";
        out.write(msg.getBytes());
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (out != null) {
            out.close();
        }
    }

}
