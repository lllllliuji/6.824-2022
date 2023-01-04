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