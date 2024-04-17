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
- 240412
    Option的枚举情况有两种 Some和None 可以通过if let/match/unwrap/？取出Some包裹的值 如果没有则是None
    Result的枚举情况有两种 Ok和Err    同样也是match/unwrap
- 240413
    ok
    精简设计
    复原
- 240414
    把EventLoop里的task_list去掉 复用EventLoop::run_once中的events临时变量
        去不掉 没地方保存
        还是得把events临时变量 变成EventLoop的成员
            Job 从Box 改成 Arc<Mutex>
    update函数 也用tokenallocator打通 这样全局所有的token大一统 即local线程变量管理所有类型 并给每个类型分配唯一id 根据id找到具体的类型
              +--type
              |
    token/id -+   (allocator分配/token/id)
              |
              +-token

        +--token
        |       
    fd -+
        |
        +--job
    events.get_mut时 得到的是token/id结构即FdEntry(名字不好改成JobEntry)
    复原
- 240415
    JobEntry 添加event
    update函数 也用tokenallocator打通 这样全局所有的token大一统 即local线程变量管理所有类型 并给每个类型分配唯一id 根据id找到具体的类型
    根据设计 只有当poll queue的时候 才会把job给push到es的events里 所以注册job的时候 job应该保存到queue里
    否则就是 get_job/set_job那一套
    poll.rs拆分
- 240417
    强引用死循环?

    onAcceptConnection
    stream -> Arc<conn> -> session
                  conn <- session    // session会赋给conn->read/write_job成员
                            register epoll hashmap: <fd, Job(Arc<conn>)>
                                                    // fd有事件到来则回调connection读写框架 框架里面会调用上层session的job

    Job(Arc(conn)) 入队
        执行发送函数
    发送完毕解引用
        stream -> Arc<conn> -x-> session


    conn一直引用session  且无释放时机(只有当conn释放本身时才会释放自身的read/writ_job 从而解引用session)
    数据发送完毕 session手动解引用 Arc<conn>

