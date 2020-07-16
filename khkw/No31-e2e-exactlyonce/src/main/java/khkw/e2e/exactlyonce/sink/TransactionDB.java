package khkw.e2e.exactlyonce.sink;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.*;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.sink
 * 功能描述:
 * 操作步骤:
 * <p>
 * 作者： 孙金城
 * 日期： 2020/7/16
 */
public class TransactionDB {
    private Map<String, List<Tuple3<String, Long, String>>> transactionContent = new HashMap<>();

    public static TransactionDB instance = new TransactionDB();

    private TransactionDB(){}

    /**
     * 创建当前事物的临时存储
     */
    public TransactionTable createTable(String transcationId) {
        transactionContent.put(transcationId, new ArrayList<>());
        return new TransactionTable(transcationId, instance);
    }

    /**
     *
     */
    public void persist(String transcationId) {
        List<Tuple3<String, Long, String>> content = transactionContent.get(transcationId);
        Iterator<Tuple3<String, Long, String>> it =  content.iterator();
        while (it.hasNext()){
            System.err.println(it.next());
        }
        transactionContent.remove(transcationId);
    }

    public void putRecord(String transcationId, List<Tuple3<String, Long, String>> values) {
        List<Tuple3<String, Long, String>> content = transactionContent.get(transcationId);
        content.addAll(values);
    }
}

