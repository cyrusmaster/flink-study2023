package cn.cyrus.chapter12;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



 /**
  * REMARK  无界流
  * netcat 工具 做监听   nc -lk    需要先启动 否则报连接异常
  * @className   StreamWordCount
  * @date  2022/7/22 21:27
  * @author  cyf
  */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        //README.md 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.setParallelism(4);

        //2 数据源转换批流
        // String path = "D:\\project\\flink\\study\\class1\\src\\main\\resources\\hello.txt";
        // DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        //parameter tool 配置参数    程序外部传参或者 idea运行配置 --host localhost --port 7777
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // socket流读取数据   netcat
        DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);



        //3 算子transformation
        //流没有group by（针对一批数据分组）  流数据时数据没有到 按照key划分（按照数据hashcode进行分区）
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = dataStreamSource
                .flatMap(new WordCount.MyFlatMapper()).keyBy(data -> data.f0).sum(1);

        //4 定义返回 结果增量更新  num>  为线程编号 分区编号  默认并行度为机器核心数
        resultStream.print();

        //5 启动流计算
        env.execute();


    }
}
