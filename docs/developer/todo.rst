####
Todo
####

FastData
========

Multiday file handling
======================
The project needs a way to pass more than just "today" and "yesterday" to the codes.

Adding "previous" and "next" columns to the product process link may be a way of handling this("previous=2" would mean "to make a product of date 2018-01-15, hand in 2018-01-13 and 2018-01-14 of the input product as well at 2018-01-15" and "next=1" would put in 2018-01-16.)

This would require establishing a previous/next(chronologically) relationship. The currently proposed idea is to add a new table to the database similar to the existing filefilelink table, which has a file's id and it's previous file id, adding to this table in the same place to filefilelink table is added to.
