package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

// MYDB 支持的最低的事务隔离程度，是“读提交”（Read Committed），即事务在读取数据时, 只能读取已经提交事务产生的数据
// （防止级联回滚与 commit 语义冲突）

// XMIN 应当在版本创建时填写，而 XMAX 则在版本被删除，或者有新版本出现时填写。
// XMAX 这个变量，也就解释了为什么 DM 层不提供删除操作，当想删除一个版本时，只需要设置其 XMAX，这样，这个版本对每一个 XMAX 之后的事务都是不可见的，也就等价于删除了。
public class Visibility {

    // 解决版本跳跃的思路也很简单：如果 Ti 需要修改 X，而 X 已经被 Ti 不可见的事务 Tj 修改了，那么要求 Ti 回滚

    // 版本跳跃的检查
    // 取出要修改的数据 X 的最新提交版本，并检查该最新版本的创建者对当前事务是否可见:
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {  // 读提交允许版本跳跃
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    // 若条件为 true，则版本对 Ti 可见。
    // 那么获取 Ti 适合的版本，只需要从最新版本开始，依次向前检查可见性，如果为 true，就可以直接返回
    // 以下方法判断某个记录对事务 t 是否可见：
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;   // 由t创建，且还未被删除，则可见

        if(tm.isCommitted(xmin)) {
            if(xmax == 0) return true;  // 由已提交事务创建，且还未被删除，则可见
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;    // 由已提交事务创建，由未提交事务删除，则可见
                }
            }
        }
        return false;
    }

    // 事务只能读取它开始时, 就已经结束的那些事务产生的数据版本
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true; // 由t创建，且还未被删除，则可见

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) return true;  // 由已提交事务创建，且该事务小于t，且该事务在t开始前提交，且未被删除，则可见
            if(xmax != xid) {   // 由已提交事务创建，且该事务小于t，且该事务在t开始前提交，由其他事务删除
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {   // 删除该记录的事务还未提交，或该事务晚于t开始，或该事务在t开始时还未提交，则可见
                    return true;
                }
            }
        }
        return false;
    }

}
