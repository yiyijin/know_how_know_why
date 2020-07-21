package upgrade;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - upgrade
 * 功能描述: 演示开启Checkpoint之后,failover之后可以从失败之前的状态进行续跑。
 * 操作步骤:
 *        0. 进行SavepointForRestore的测试之后，进行这个测试
 *        1. 打包： mvn clean package
 *
 *        -d means it will not wait for the job to finish to return, rather it will just return the job ID
 *        e.g. Job has been submitted with JobID 287aa21de22a5cafbfb5f8fd495c98b1
 *
 *        2. 其中作业：bin/flink run -d -m localhost:4000 -c upgrade.SavepointForRestoreWithoutException /Users/ejin/study/alibaba-sun-jin-cheng/know_how_know_why/khkw/No25-upgrade/target/No25-upgrade-0.1.jar
 *
 *        savepoint followed by jobID which is obtained from the command line output
 *        3.创建savepoint： bin/flink cancel -s 287aa21de22a5cafbfb5f8fd495c98b1
 *        output will be
 DEPRECATION WARNING: Cancelling a job with savepoint is deprecated. Use "stop" instead.
 Cancelling job 287aa21de22a5cafbfb5f8fd495c98b1 with savepoint to default savepoint directory.
 Cancelled job 287aa21de22a5cafbfb5f8fd495c98b1. Savepoint stored in file:/tmp/chkdir/savepoint-287aa2-ccb4df40caaa.
 *
 *
 *        4. 停止以前的作业，然后从savepoint启动
 *        6. bin/flink run -m localhost:4000 -s file:///tmp/chkdir/savepoint-287aa2-ccb4df40caaa -c upgrade.SavepointForRestoreWithoutException /Users/ejin/study/alibaba-sun-jin-cheng/know_how_know_why/khkw/No25-upgrade/target/No25-upgrade-0.1.jar
 * 作者： 孙金城
 * 日期： 2020/6/29
 */
public class SavepointForRestoreWithoutException {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 打开Checkpoint, 我们也可以用 -D <property=value> CLI设置
        env.enableCheckpointing(20);
        // 作业停止后保留CP文件
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStream<Tuple3<String, Integer, Long>> source = env
                .addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
                        int index = 1;
                        while(true){
                            ctx.collect(new Tuple3<>("key2", index++, System.currentTimeMillis()));
                            ctx.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
                            // Just for testing
                            Thread.sleep(100);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                });
        source.keyBy(0).sum(1).print();

        env.execute("SavepointForFailoverWithoutException");
    }
}
