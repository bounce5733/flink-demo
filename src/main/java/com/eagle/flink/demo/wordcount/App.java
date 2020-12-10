package com.eagle.flink.demo.wordcount;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * TODO
 *
 * @Author jiangyonghua
 * @Date 2020/9/8 21:01
 * @Version 1.0
 **/
@Slf4j
public class App {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Word[] data = new Word[]{new Word(1, "Hello", 1), new
                Word(1, "World", 3), new Word(2, "Hello", 1)};
        env.fromElements(data).keyBy(new MyKey())
                .max("frequency")
                .addSink(new SinkFunction<Word>() {
                    @Override
                    public void invoke(Word value, Context context) throws Exception {
                        log.info("value:{}", value);
                    }
                });
        env.execute("统计最大值");
    }
}

@AllArgsConstructor
@Data
class Word {
    private int id;
    private String name;
    private int frequency;
}

class MyKey implements KeySelector<Word, String> {

    @Override
    public String getKey(Word word) throws Exception {
        return word.getName();
    }
}
