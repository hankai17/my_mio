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

- session模板对象 解耦玩法

