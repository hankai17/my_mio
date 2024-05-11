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
- 240414
    把EventLoop里的task_list去掉 复用EventLoop::run_once中的events临时变量
        去不掉 没地方保存
        还是得把events临时变量 变成EventLoop的成员
            Job 从Box 改成 Arc<Mutex>
    update函数 也用tokenallocator打通 这样全局所有的token大一统 即local线程变量管理所有类型 并给每个类型分配唯一id 根据id找到具体的类型
                            +--type
                            |
    <Token/id,  TokenEntry>-+   (allocator分配/token/id)
                            |
                            +-token

                  +--Token
                  |       
    <fd, JobEntry-+>
                  |
                  +--job
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

    events.get_mut时 得到的是token/id结构即FdEntry(名字不好改成JobEntry)
    复原 notify timer channel
- 240418
    register函数参数中去掉token?
        建议保留 参数的token可以作为 该类型的token
- 240420
    复原 notify timer channel
    register函数参数中去掉token?
- 240422
    register函数参数中去掉token?
    event_loop 队列整理 
    文件拆分
    全局<Token/id, TokenEntry> 生命周期管理
- 240423
    register(...token...) => register(...type...)
    复原 notify timer channel
- 240424
                                +--type
                  +--TokenEntry-+
                  |             +--id
    <fd, JobEntry-+>
                  |
                  +--job

    let (registration, set_readiness) = Registration::new(poll, token.token, interest, opts);
    改成
    let (registration, set_readiness) = Registration::new(poll, TokenEntry, interest, opts);
- 240425
    只有超时 没有回调?  timer_list 实现
    trigger 添加
    批量数据发送

- 240430
    conn 支持timeout功能

- 240506
    conn 支持timeout功能
    分文件 + 删除注释
    添加代理案例
    消除编译告警
- 240507
    notify实现 --> 删除notify
    timer设计理念
        设计理念是 首先有一个最小定时器 stat 用于记录最接近的要发生的定时器时间 仅仅用于readiness触发es
        触发后 则进行具体的poll_to

- 240508
    es系统的timer类型的任务 需要遍历timers全局队列
              +--------+
              |        |
    <r     ,  |   s>   |  ----> timer.poll_to
TimersEvent   |        |
              +--------+
              |   ES   |
              +--------+


    es系统的other类型的一次性任务 直接回调
              +--------+
              |        |
    <r     ,  |   s>   |  ----> cb()
OthersEvent   |        |
              +--------+
              |   ES   |
              +--------+
- 240509
    mpsc 入队
    +--+
    +__+
    head/tail指向本身 本身在源码中即是end_marker

    +--+
    +__+ -nxt->  +--+
     ^           +--+
     |            ^
    tail          |
    head----------+

    +--+
    +__+ -nxt->  +--+  -nxt-> +--+
     ^           +--+         +--+
     |                         ^
    tail                       |
    head------------------------

    mpsc 出队

- 240510
    ES层        如果epoll有超时时间 且队列消费完毕 那么就将标准节点换成sleep
    业务层      enqueue node时 则只通知一次 减少系统调用
    ES层        dequeue node时 当消费完了 则重新替换成end
                               如果enqueue的node非常多 本轮次消费不完 下一轮prepare_for_sleep 返回false 设置epoll立即返回
    多线程支持: 主进程拉起多线程poll
    log支持
    handler细化

- 240511
    src/runtime/io/registration_set.rs:26:    pending_release: Vec<Arc<ScheduledIo>>,
        ScheduleIo 没有&mut self
