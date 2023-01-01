lab 3A
* basic test, client doesn't check reply, need retry put append operation if reply.success == false, fixed
* deadlock start agreement could fail(leader change), it's not good to use cond.wait, never wakeup, use periodly check found this bug by monitor applych
* applyCh, decouple
* leader change, return quickly and retry another server
* deadlock, 