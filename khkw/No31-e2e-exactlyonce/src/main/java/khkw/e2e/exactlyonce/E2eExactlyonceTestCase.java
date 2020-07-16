package khkw.e2e.exactlyonce;

import khkw.e2e.exactlyonce.functions.MapFunctionWithException;
import khkw.e2e.exactlyonce.functions.ParallelCheckpointedSource;
import khkw.e2e.exactlyonce.functions.StateProcessFunction;
import khkw.e2e.exactlyonce.functions.Tuple3KeySelector;
import khkw.e2e.exactlyonce.sink.E2EExactlyOnceSinkFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.concurrent.TimeUnit;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.functions
 * 功能描述:
 * 操作步骤:
 * <p>
 * 作者： 孙金城
 * 日期： 2020/7/16
 */
public class E2eExactlyonceTestCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStateBackend(new FsStateBackend("file:///tmp/chkdir", false));
        env.enableCheckpointing(1000);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(1, TimeUnit.MILLISECONDS) ));

//        atLeastOnce(env);
//        exactlyOnce(env);
        e2eExactlyOnce(env);

        env.execute("E2e-Exactly-Once");
    }

    private static void atLeastOnce(StreamExecutionEnvironment env) {
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        baseLogic(env).print();
    }

    private static void exactlyOnce(StreamExecutionEnvironment env) {
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        baseLogic(env).print();
    }

    private static void e2eExactlyOnce(StreamExecutionEnvironment env) {
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        baseLogic(env).addSink(new E2EExactlyOnceSinkFunction());
    }

    private static SingleOutputStreamOperator baseLogic(StreamExecutionEnvironment env) {
        DataStreamSource<Tuple3<String, Long, String>> source1 =
                env.addSource(new ParallelCheckpointedSource("S1"));
        DataStreamSource<Tuple3<String, Long, String>> source2 =
                env.addSource(new ParallelCheckpointedSource("S2"));
        SingleOutputStreamOperator ds1 = source1.map(new MapFunctionWithException(10));
        SingleOutputStreamOperator ds2 = source2.map(new MapFunctionWithException(200));
        return ds1.union(ds2)
                .keyBy(new Tuple3KeySelector())
                .process(new StateProcessFunction());
    }
}
