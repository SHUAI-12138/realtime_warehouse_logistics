package com.atguigu.realtime.util;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

public class DorisUtil {

    public static DorisSink<String> getDorisSink(String table) {
         return new DorisSink<>(
                DorisOptions
                        .builder()
                        .setFenodes(PropertyUtil.getStringValue("DORIS_FE"))
                        .setTableIdentifier(table)
                        .setUsername(PropertyUtil.getStringValue("DORIS_USER"))
                        .setPassword(PropertyUtil.getStringValue("DORIS_PASSWORD"))
                        .build(),
                DorisReadOptions.builder().build(),
                DorisExecutionOptions
                        .builderDefaults()
                        .disable2PC()
                        .build(),
                new SimpleStringSerializer()
        );
    }

}
