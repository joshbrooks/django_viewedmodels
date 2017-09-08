============
ViewedModels
============

Viewed Models is a simple way to add PostGres "views" which look to Django just like Models.
This lets us go outside the Django ORM to create views or materialized views to harness
the power of PostGreSQL without losing the power to access that data using the ORM.

Usage:
 - A model which uses this framework should inherit from ViewedModel
 - The model requires an "sql" method which returns the sql required to create view
 - The model also requires a "dependencies" attribute. These are useful in generating table names within the SQL statement as well as dependency resolution.
 - The model also requires fields specified in the standard Django way. Foreign keys should work fine
     - Foreign Keys: For ``ForeignKey(myApp.MyModel')`` we need to have a field 'mymodel_id' returned from the SQL.

 - Every Django model (including these ViewedModels) requires an 'id' field. We can fake this by including ``row_number() OVER () AS id`` somewhere in our SELECT statement.
