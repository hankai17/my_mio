# my_mio
- 整体架构
    Selector 仅包装efd
- 新架构
	Fd
	Socket opts  \/
	Socket/Connection 封装 buffer + CB(epoll_cb + session_cb)
	Session 封装Socket/Connection + 上层cb
	
- 是否暴露事件
	如何交友上层(Connection层或Session层)设置事件监听

- 强/弱引用 + 函数对象玩法
node引用queue node中的readiness_queue是AtomicPtr<()>类型
初始化node的queue时 传参为*mut()
*mut() 可以强转为 Arc<ReadinessQueueInner> 从而调用RQI的各种操作  RQI的函数均均均为只读的!!! 
为什么只读可以实现可写的功能? 是因为用了强转裸指针+unsafe技术 (参考test/rust/day01/11type.rs)

- session模板对象 解耦玩法

