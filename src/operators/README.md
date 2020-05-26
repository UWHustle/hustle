# Notes

* It is difficult to guarantee that the aggregates are outputted in the correct 
order without introducing another mutex. Instead, we can simply store the correct
ordering of the aggregates throughout execution and then sort our output at the end.
* Why not reserve space in a vector? Need vectors of varying type.
* You can compute all possible groups before computing the aggregates. Then you
won't have to worry about types, and you also won't have to worry about varchar
groups, since you already know the offsets. Why not do that? If we find that a group 
has an empty filter, then it should be excluded from the output. however, we 
would have already reserved space for it. We would need to delete this group at 
the end anyways. We would delete it using the same approach we are proposing now.
The former approach achieves the same end with more work.
