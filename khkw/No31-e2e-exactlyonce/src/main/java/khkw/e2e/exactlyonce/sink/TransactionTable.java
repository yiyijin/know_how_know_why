package khkw.e2e.exactlyonce.sink;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.sink
 * 功能描述: 这是e2e Exactly-once 的临时表抽象。
 * 作者： 孙金城
 * 日期： 2020/7/16
 */
public class TransactionTable {
    private final TransactionDB db;
    private final String transactionId;
    private final List<Tuple3<String, Long, String>> buffer = new ArrayList<>();

    public TransactionTable(String transcationId, TransactionDB db) {
        this.transactionId = transcationId;
        this.db = db;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public TransactionTable insert(Tuple3<String, Long, String> value) {
        buffer.add(value);
        return this;
    }

    public TransactionTable insertAll(Collection<Tuple3<String, Long, String>> values) {
        values.forEach(this::insert);
        return this;
    }

    public TransactionTable flush() {
        db.putRecord(transactionId, buffer);
        return this;
    }

    public void close() {
        buffer.clear();
    }
}
