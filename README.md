Performs DP or double propagation of scalar data to grandparent node. Only suitable for cases where grantparent has an edge type that has an embedded 1:1 relationship with a child node or in this case the grantdchild.

This program can only be used after the intial load of the graph. It cannot handle incremental dp updates - a separate program will be developed for this purpose.
