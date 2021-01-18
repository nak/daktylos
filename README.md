# daktylos
Daktylos can be used with sqlalchemy-supported databases, and comes with MySQL and Amazon Redshift support.  To use the SQL modules, there are separate requirements-sql.txt and requkirement-redshift.txt files to install dependencies.  Database creation is automatically handled when connecting to the database, if preferred.  

To connect to a database to store composite metrics, store the composite metrics in a hiearchical Python dataclass and post. For example, if you have an instance of a dataclass, "metricdata", to store, and an engine created via sqlalchemy, 
the code would look like:

from daktylos.data_stores import SQLMetricStore

with SQLMetricStore(engine, create=True) as datastore:
    datastore.post(metric_name="TopLevelMetricName", metric_data=metricdata)
    
    
The post call can also take a dict of metadata, whose keys are of type str and whose value are of type str or int.  This 
metadata will be associated with the composite metric and can be recalled along with the metric data when queried.  It can 
also take a specific timestamp, which defaults to "now" if not specified.

The datastore object can also be used to retrieve data as an array of composite metrics, as array of each individual field of the composite (making it easier to use more readily in plotting), and each of these can be filtered against metadata fields that match specific values.



                   
