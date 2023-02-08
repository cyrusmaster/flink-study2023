package cn.cyrus.chapter12;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
  * REMARK  有界的流
  * https://www.bilibili.com/video/BV133411s7Sa?p=12&spm_id_from=pageDriver&vd_source=5071a562e0eb15369b754fe144a86f67

 * 结论一：并行子任务编号  并行度  默认cpu线程数
 * 结论二：sum 聚合算子 任务运行同一个线程 便于累加
 *
  * @className   BoundedStreamWordCount
  * @date  2022/7/22 20:06
  * @author  cyf
  */

public class BoundedStreamWordCount {

     public static void main(String[] args) throws Exception {

         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

         DataStreamSource<String> stringDataStreamSource = env.readTextFile("src/main/resources/hello.txt");


         SingleOutputStreamOperator<Tuple2<String, Long>> returns = stringDataStreamSource.flatMap((String in, Collector<Tuple2<String, Long>> out) -> {
                     String[] words = in.split(" ");
                     for (String word : words) {
                         out.collect(Tuple2.of(word, 1L));
                     }
                 })
                 .returns(Types.TUPLE(Types.STRING, Types.LONG));

         KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = returns.keyBy(line -> line.f0);

         SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);

         sum.print();
         env.execute();

     }


}
