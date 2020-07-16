package khkw.e2e.exactlyonce.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.UUID;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.functions
 * 功能描述:
 * TwoPhaseCommitSinkFunction有4个方法:
 * - beginTransaction() 开启事务.创建一个空的List
 * - preCommit() - flush并close这个文件,之后便不再往其中写数据.同时开启一个新的事务供下个checkponit使用
 * - commit() - 把pre-committed的临时文件移动到指定目录
 * - abort() - 删除掉pre-committed的临时文件
 * <p>
 * 作者： 孙金城
 * 日期： 2020/7/16
 */
public class E2EExactlyOnceSinkFunction extends
        TwoPhaseCommitSinkFunction<Tuple3<String, Long, String>, TransactionTable, Void> {

    public E2EExactlyOnceSinkFunction() {
        super(new KryoSerializer<>(TransactionTable.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(TransactionTable table, Tuple3<String, Long, String> value, Context context) throws Exception {
        table.insert(value);
    }

    @Override
    protected TransactionTable beginTransaction() {
        return TransactionDB.instance.createTable(UUID.randomUUID().toString());
    }

    @Override
    protected void preCommit(TransactionTable table) throws Exception {
        table.flush();
        table.close();
    }

    @Override
    protected void commit(TransactionTable transaction) {
        TransactionDB.instance.persist(transaction.getTransactionId());
    }

    @Override
    protected void abort(TransactionTable transaction) {
        transaction.close();
    }
}
