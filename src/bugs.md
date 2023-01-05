
### lab 2A
* rand electiontimeout
* votedFor = -1, if reply's term greater than currentTerm


### lab 2B
* disconnect, leader election increment term, back and leader changed, same term, apply wrong log
* 

### lab 2C
* accelebrate log backtracking, border case
* log {1, 2, 3}, log{1, 2, 3, 4, 5} reply order, update nextIndex wrong
* turn to follower, votedfor = -1
* replicate log unit = throught up to leader's newest log entry

### lab 2D
* deadlock, caused by snapshot, 
* If commitIndex > lastApplied: increment lastApplied, applylog[lastApplied] to state machine
* test code snapshot in for operation of channel, if apply log in a for loop. cause deadlock
* start a backgroud goroute apply logs (lastApplied, commitIndex]
* doesn't need to wait sendAppendEntries, just send and carefully treat reply, detect this bug because after reconnect, time costs very high
* persist Snapshot attributes only when snapshot
* snapshot doesn't work with applylog together

### others
* applychan in critcial zone, cause deadlock 
* infinite loop result in deadlock
* 每个方法的开头和结束打上 ping pong日志，如果不配对说明有死锁

lab 3A
* basic test, client doesn't check reply, need retry put append operation if reply.success == false, fixed
* deadlock start agreement could fail(leader change), it's not good to use cond.wait, never wakeup, use periodly check found this bug by monitor applych
* applyCh, decouple
* leader change, return quickly and retry another server
* deadlock, kv startagreement to raft, might fail, cond.wait never wake up, solution: periodlly broadcast all cond

lab 3B
* duplicate log entry in raft
* follower snapshot index might > leader snapshot, it's ok
* follower lose update and became leader, missing element. cause: snapshot apply use the same channel as log apply, carefully deal with order
* beacause of latency, kvserver read snapshot whose snapshot index > kvserver's commitedIndex

lab 4A
* rebalance should be determinstic, stable sort matters a lot
* operation result should be deterministic