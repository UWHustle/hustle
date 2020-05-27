# Notes

* If you use a global scheduler for all the queries without calling `reset()` on the scheduler, your OperatorResult pointers will be deallocated randomly.
