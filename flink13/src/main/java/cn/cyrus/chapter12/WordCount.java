package cn.cyrus.chapter12;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * REMARK
 * @className   WordCount
 * @date  2021/11/11 20:17
 * @author   chenyongfeng
 *  遇事不决量子力学
 */

//批处理
public class WordCount {

    public static void main(String[] args) throws Exception {


        //README.md 创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "src/main/resources/hello.txt";
        DataSource<String> stringDataSource = env.readTextFile(inputPath);

        // 对数据集处理 按空格分词 转换（word num）二元组
        //flatmap 一条数据 多个结果    按照第一个位置 word分组  第二个位置求和
        AggregateOperator<Tuple2<String, Integer>> sum = stringDataSource
                .flatMap(new MyFlatMapper()).groupBy(0).sum(1);


        //结果一下全展示出来
        sum.print();


        //env.execute();

    }


    // 自定义类 实现flatMapFuncation 接口   使用flink自定义元组类对象
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {


        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        //    按空格分词
          String[] words = s.split(" ");
        // 遍历所有words 转换成二元组
            for (String word:words) {
                collector.collect(new Tuple2<>(word,1));

            }
        }
    }

}
