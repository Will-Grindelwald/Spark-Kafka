添加新的信号时, 记得

* 修改 cn.ac.sict.store.Store.java 的 内部类 PutFunction 的 call 方法
* 修改 cn.ac.sict.main.Main.java 的 Store.toHBase() 的 Class 参数

否则不能完整存储到 hbase