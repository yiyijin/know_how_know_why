package khkw.e2e.exactlyonce.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.functions
 * 功能描述: 端到端的精准一次语义sink示例（测试）
 * TwoPhaseCommitSinkFunction有4个方法:
 * - beginTransaction() Being called on initializeState/snapshotState
 *  called in initializeState so that can start a new transactionId
 *  called in snapshotState, as the current cp finishes, and next cp will begin, so that start a new transactionId for next cp
 * - preCommit() Being called on snapshotState
 *  snapshotState is to do the cp, whether it succeeds or not, we dont know, so we call preCommit to write data to a temp storage
 * - commit()  Call on notifyCheckpointComplete()
 * - abort() Call on close()
 *
 * ?? reason why transactionId needs to be recorded is, there might be multiple cp finished say cp1 and cp2 before commit failed,
 * and each cp will generate a new transactionId
 * and commit failed for transactionId1, which is corresponding to cp1, so use that transactionId1 to find the data
 * <p>
 * 作者： 孙金城
 * 日期： 2020/7/16
 */
public class E2EExactlyOnceSinkFunction extends
        TwoPhaseCommitSinkFunction<Tuple3<String, Long, String>, TransactionTable, Void> {

    private static AtomicInteger PRE_COMMIT = new AtomicInteger(0);
    private static AtomicInteger COMMIT = new AtomicInteger(0);

    public E2EExactlyOnceSinkFunction() {
        super(new KryoSerializer<>(TransactionTable.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(TransactionTable table, Tuple3<String, Long, String> value, Context context) throws Exception {
        table.insert(value);
    }

    /**
     * Call on initializeState and snapshotState
     */
    @Override
    protected TransactionTable beginTransaction() {
        return TransactionDB.getInstance().createTable(
                String.format("TransID-[%s]", UUID.randomUUID().toString()));
    }

    /**
     * Call on snapshotState
     */
    @Override
    protected void preCommit(TransactionTable table) throws Exception {
        /*if (PRE_COMMIT.getAndIncrement() > 0) {
            throw new RuntimeException("simulated error in commit");
        }*/
        table.flush();
        table.close();
    }

    /**
     * Call on notifyCheckpointComplete(), checkpoint complete means the whole flink job's checkpoint, not just for this operator
     */
    @Override
    protected void commit(TransactionTable table) {
        System.err.println(String.format("SINK - CP SUCCESS [%s]", table.getTransactionId()));
        // if commit has exception, it will try to restore from previous checkpoint
        if (COMMIT.getAndIncrement() > 0) {
          throw new RuntimeException("simulated error in commit");
        }
        TransactionDB.getInstance().secondPhase(table.getTransactionId());
    }

    /**
     * Call on close()
     */
    @Override
    protected void abort(TransactionTable table) {
        TransactionDB.getInstance().removeTable("Abort", table.getTransactionId());
        table.close();
    }
}
