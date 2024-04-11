# my_mio
- 240329
    notify测试
    编译警告 代码清理
    多线程poll()测试
    添加多线程poll listener功能
    proxy测试
    ares/ss5/kcp扩展
    http/websocket/http2
    
- 240330
    u64
- 240408
    timer测试ok
    timer加入tcpserver
    决定移除token
        c里可以直接用原始指针 将上层vc抽象保存到epoll_event里
        原始代码里用token 模仿之
        <fd, job>       ===>    <fd, token> --ready_list<token, job>--> job
        <token, job>    ===>    <token, job>

        token_alloc 线程共享
            uint64_t[atomic]: <type, true_val>
            type:               true_val:
                sock_event          token (这个token源于当前线程的 ready_list<token, job>)
                notify              token
                timer               token (这个token源于当前线程的 timer_list<token, job>) 每添加一个定时器则往timer_list中插入一个job 且生成一个token
                job                 token (这个token源于当前线程的   job_list<token, job>) 每添加一个同步任务 则往job_list中插入一个job 且生成一个token

        fn set(type, token) -> token // 原子++ // 拿到返回值token后 设置到epoll_event里
        fn get(token) -> <type, true_token> // 然后根据不同的type找不同的队列
        fn free(token)
        模拟epoll.rs中的NEXT_ID
    ready_list timer_list job_list 全局 or event_loop
    epoll 传出队列
- 240410
    重构传出的Events
- 240411
                  +--type
        +--token--+
        |         +--token
    fd -+
        |
        +--job

    Events 生命周期管理
    添加slab队列
    调通

        

