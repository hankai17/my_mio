# my_mio vs mio 0.6 对比

> 整理时间：2026-10-05
> 对比对象：`/home/melon/my_mio`（Rust 复刻 mio 0.6 并加高层封装的网络框架） vs `/home/melon/my_mio/mio-0.6.x`（官方 mio 0.6 源码）

---

## 一、一句话结论

**my_mio 和 mio 0.6 的最大不同是"定位"——mio 是库（给原语），my_mio 是框架（给封装）。** 由此派生出技术上的根本分歧：事件分发从"token 交给用户查表"变成了"fd 交给框架查表回调"。

```
              mio 0.6（库）              my_mio（框架）
目标          通用、可复用               开箱即用、像 muduo
代价          用起来繁琐                  unsafe + 锁 + 死锁 + 复用坑
扩展          Evented 开放                Handler/Job 封闭
成果          成为生态基石                学习价值高，工程价值弱
```

---

## 二、定位与架构对比

| 维度 | mio 0.6 | my_mio |
|---|---|---|
| 定位 | **库**，提供原语 | **框架**，提供高层封装 |
| 给你什么 | `Poll`/`Registration`/`Evented`/`Token` | `EventLoop`/`TcpServer`/`TcpConnection`/`TcpClient`/`Connector`/`Acceptor` |
| 事件分发 | 返回 token，**用户自己 `match` 查表** | 框架查 `events_map`，**直接执行闭包回调** |
| `Selector` 状态 | `id` + `epfd`，**无状态** | `id` + `epfd` + `events_map: HashMap`，**有状态** |
| 锁 | epoll 层**无锁**（状态全在内核） | `Arc<Mutex<HashMap>>`（用户态维护表） |
| token | 核心抽象，用户分配 | 退化为 `TokenEntry`（类型标签），半废弃混乱 |

一句话：**mio 让你自己搭积木，my_mio 想替你搭好房子。** 前者克制，后者贴心但背上了"替你查表、替你管生命周期"的全部代价。

---

## 三、技术差异对照

| 项 | mio 0.6 | my_mio |
|---|---|---|
| 注册数据存哪 | 内核红黑树（`data.u64` 存 token） | 用户态 `events_map`（fd→JobEntry） |
| epoll_wait 后 | 还原 token | 还原 fd → 查表找 JobEntry |
| 回调机制 | 无（用户自己处理事件） | `Job = Arc<Mutex<dyn FnMut(i64)>>` 闭包 |
| 线程模型 | `Poll` Send+Sync，register 任意线程，poll 单线程 | `EventLoopPool` one-loop-per-thread + `get_random_poller` |
| 跨事件系统 | `SelectorId` 禁止一 fd 多 Poll | 复刻了 `SelectorId`（行为一致） |
| 多线程 poll | `lock_state` + condvar 串行化 | 同样复刻（`poll1`） |
| 高层抽象 | 无 | `Handler`/`ClientHandler` trait |

---

## 四、关键机制逐项对比

### 1. 事件分发模型（最大技术分歧）

**mio 0.6**：token 是核心抽象，框架不查表。

```rust
// 用户注册
poll.register(&stream, Token(0), Ready::readable(), PollOpt::edge())?;
// 用户分发
for event in &events {
    match event.token() {
        Token(0) => { /* 处理连接 0 */ }
        _ => {}
    }
}
```

**my_mio**：fd 是 key，框架查表执行闭包。

```rust
// 注册即挂闭包
poll.register(&stream, token_entry, interest, opts, job);  // job = 回调闭包
// 框架内部
let fd = epoll_event.u64 as i32;
map.get_mut(&fd) -> JobEntry.job.lock()(ready);  // 直接执行
```

### 2. `Selector` 状态与锁

| | mio 0.6 | my_mio |
|---|---|---|
| `Selector` 字段 | `id` + `epfd` | `id` + `epfd` + `events_map: Arc<Mutex<HashMap<i32, JobEntry>>>` |
| 是否需要锁 | 不需要（无共享表） | 需要（`Arc<Mutex<HashMap>>`） |
| 锁的来源 | — | 编译层（HashMap 非 Sync）+ 运行层（&self 下可变） |

mio 0.6 无锁的本质：**把"fd→数据"映射完全托管给内核**，用户态零状态。my_mio 为了"事件直接回调"，在用户态重建这张表，于是必须配锁。

### 3. register 到 epoll 的链路

两者最终都落到 `epoll_ctl`，但语义不同：

```
mio 0.6:  Poll::register → Evented::register → Selector::register
          → epoll_ctl(ADD)  // data.u64 = token，用户态不存东西

my_mio:   Poll::register → Evented::register → Selector::register
          → events_map.insert(fd, JobEntry) + epoll_ctl(ADD)  // data.u64 = fd
```

### 4. connect 机制

mio 0.6 只提供底层原语（非阻塞 connect + writable 事件 + take_error 确认）：

```
① TcpStream::connect(&addr)   // 非阻塞发起，可能没连上（EINPROGRESS 当正常）
② poll.register(&stream, token, Ready::writable(), edge)
③ poll.poll(&events)          // 等到 writable（握手完成）
④ stream.take_error()         // 读 SO_ERROR 确认成功/失败
```

my_mio 把这三步曲封装成 `Connector`（含超时机制），`TcpClient::start_connect` 内部 `take_error` 判失败。

### 5. 线程安全模型

| | mio 0.6 | my_mio |
|---|---|---|
| `Poll` Send+Sync | ✅ 自动（无 UnsafeCell） | ❌ 手动 `unsafe impl`（因 `UnsafeCell<Timer>`） |
| register 线程安全 | ✅ 官方明说 thread safe（epoll_ctl 内核保证） | ⚠️ 靠 `events_map` 的 Mutex |
| poll | 单线程（内部锁串行化） | 同样 lock_state 串行化 |
| 事件处理 | 固定在 poll 线程 | 同样要求（但 `get_random_poller` 例子违反了） |

---

## 五、优缺点

### mio 0.6

| 优点 | 缺点 |
|---|---|
| 无状态、epoll 层无锁 | 只有原语，用起来繁琐（自己维护 token 表、自己写循环） |
| 内存安全、API 稳定、久经考验 | 没有 TcpServer/Client 等高层封装 |
| 可复用性极强（tokio 等大量项目的地基） | 学习曲线陡（要懂事件驱动 + token 分发） |
| 扩展缝清晰（`Evented` trait） | 无定时器/连接池等开箱能力（要自己搭） |

### my_mio

| 优点 | 缺点 |
|---|---|
| 开箱即用的 muduo 风格封装 | `unsafe impl Send/Sync` 泛滥（`UnsafeCell<Timer>` UB 隐患） |
| 事件直接回调，业务写得顺 | `events_map` 要锁、`select` 持锁遍历、fd 复用坑 |
| 学习价值高（完整啃下 mio 内核） | `Arc<Mutex>` 回调环导致死锁（5 个 deadlock commit） |
| 有定时器、多线程池、随机负载均衡 | 无测试、token 体系半废弃、依赖陈旧（net2/iovec） |
| 双层写缓冲、连接超时等贴心设计 | `reregister` 存 token 而非 fd 的 bug（静默丢事件） |

---

## 六、可复用性

| | mio 0.6 | my_mio |
|---|---|---|
| 复用性 | **极高**（库） | **低**（框架） |
| 原因 | 抽象克制，不替用户做决定，接口稳定 | 闭包回调、`Arc<Mutex>` 到处套，与具体设计强耦合 |
| 类比 | 标准库、地基 | 一栋装修好的样板房 |

mio 0.6 能成为 Rust 异步生态基石，正因为它**只做最底层、最通用的事**（fd 就绪通知），把"怎么映射到业务"完全留给上层。my_mio 则把"fd→回调"这个决定写死了，别人没法复用这套东西。

---

## 七、可扩展性

| 扩展点 | mio 0.6 | my_mio |
|---|---|---|
| 自定义事件源 | `Evented` trait，实现 register/reregister/deregister 即可 | 难：`Job` 闭包类型写死，`TokenType` 枚举硬编码 |
| 新增 IO 类型 | 套 `EventedFd` 或自定义，非常自然 | 要动 `TokenEntry`/`TokenType`/`JobEntry` 多处 |
| 高层扩展 | 自己搭（这正是 tokio 做的事） | `Handler` trait 是硬编码接口，扩展需改框架 |
| 并发模型 | 锁粒度自选 | 锁模型被 `Arc<Mutex>` 写死，难换无锁 |

mio 0.6 的 `Evented` trait 是清晰的扩展缝——timer、UDS、自定义事件都能干净地挂进去。my_mio 的扩展缝被 `Job`、`TokenType`、`Handler` 这些写死的类型堵住了。

---

## 八、总结与建议

### 核心洞察

my_mio 相对 mio 0.6 的几乎所有问题（锁、fd 复用、`reregister` bug、死锁），**全部来自同一个决定**——"在用户态重建 fd→回调映射表"。

- 这张表是"框架化"的必然代价，不是 bug；
- 但把它做安全、做高效，才是 my_mio 该补的课。

### 建议路径

| 场景 | 建议 |
|---|---|
| 要"生产可用的库" | 学 mio 0.6 的克制：无状态、无锁、token 交给用户 |
| 要"学习 + 快速搭服务" | 保留 my_mio 高层封装，但先搬掉三座山：unsafe、死锁、锁粒度 |
| 技术债清理顺序 | ① 去 `unsafe impl`；② 断 `Arc<Mutex>` 回调环；③ `events_map` 去 Arc/换 RwLock/slab |

### 一句话

**mio 0.6 把"查表"交给用户换来了无锁与通用；my_mio 把"查表"收进框架换来了便利，但代价是 unsafe、锁和死锁。** 两者没有绝对优劣，只有"库"与"框架"的取舍——而 my_mio 要成为合格的框架，还差把那张 `events_map` 管理好的功力。
