set print pretty

#b epoll_ctl
#b test_broken_pipe.rs:13
#b epoll.rs:146
#b epoll.rs:148
#b io.rs:36
b event_loop.rs:158
b prepare_for_sleep

