package day02;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Flink07_Source_Customer {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从自定义数据源读取数据
        DataStreamSource<SensorReading> sensorReadingDS = env.addSource(new MySource());

        //3.打印
        sensorReadingDS.print();

        //4.启动
        env.execute();

    }

    public static class MySource implements SourceFunction<SensorReading> {

        //定义标记,控制任务运行
        private boolean running = true;

        //定义一个随机数
        private Random random = new Random();

        //定义基准温度
        private Map<String, SensorReading> map = new HashMap<>();


        @Override

        public void run(SourceContext<SensorReading> ctx) throws Exception {

            //给各个传感器赋值基准值
            for (int i = 0; i < 10; i++) {
                String id = "sensor_" + (i + 1);
                map.put(id, new SensorReading(id, System.currentTimeMillis(), 60D + random.nextGaussian() * 20));
            }

            while (running) {

                //造数据
                for (String id : map.keySet()) {

                    //写出数据
                    //获取上一次温度
                    SensorReading sensorReading = map.get(id);
                    Double lastTemp = sensorReading.getTemp();
                    SensorReading curSensorReading = new SensorReading(id, System.currentTimeMillis(), lastTemp + random.nextGaussian());

                    //写出操作
                    ctx.collect(curSensorReading);

                    //更新Map内容
                    map.put(id, curSensorReading);
                }

                Thread.sleep(2000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
