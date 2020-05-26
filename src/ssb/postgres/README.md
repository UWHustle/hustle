# Changes to the standard SSB queries

* All aggregates are changed to `sum(lo_revenue)` since the only arithmetic operation Arrow supports is addition.
* All ORDER BY clauses are sorted in ascending order