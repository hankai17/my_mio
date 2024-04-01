# my_mio
- 240329
    粗暴设计 不设计ready_list 直接回调
    只有job使用token 其它一概不允许使用token
    token_alloc 确保token唯一性
        uint64_t: <type, true_val>
    timer测试
    notify测试
    代码清理
    多线程poll()测试
    添加多线程poll listener功能
    proxy测试
    ares/ss5/kcp扩展
    http/websocket/http2
    
- 230330
    u64
