set print pretty

#b epoll_ctl
#b test_broken_pipe.rs:13
#b epoll.rs:146
#b epoll.rs:86
#b io.rs:36
#b event_loop.rs:158
#b prepare_for_sleep
#b test1
#b poll1
#b poll.rs:591
#
#b AtomicState::compare_and_swap
#b AtomicState::flag_as_dropped
#b SenderCtl::inc
#b Registration::new2
#b Registration::new

#b server.rs:112
#b tcp_server.rs:126
#b event_loop.rs:183
#b tcpserver.rs:49
#b poll.rs:810
#b RegistrationInner::update
#b ReadinessQueueInner::enqueue_node
#b flag_as_dropped
b src/net/connection.rs:187
