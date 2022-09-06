Hustle Resolver
----------------

This module contains classes that are invoked from the sqlite3_exec() method, by passing parse tree as the argument.

cresolver.h contains structs and other constants that are used to represent the parse tree. So this module is used by the hustle resolver and sqlite3 code, since this is the only file in the repo containing all the struct representing the parse tree.

select_resolver module gets the parse tree and converts it into structures that can be used by the hustle operators for execution.

### Design

Image: src/resolver/sqlite3he_design.png
![Depiction of how SQLite3/HE integrates with SQLite3.](sqlite3he_design.png?raw=true)