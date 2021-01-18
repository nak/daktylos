# daktylos
Daktylos can be used with sqlalchemy-supported databases, and comes with MySQL and Amazon Redshift support.  To use the SQL modules, there are separate requirements-sql.txt and requkirement-redshift.txt files to install dependencies.  Database creation is automatically handled when connecting to the database, if preferred.  

To connect to a database to store composite metrics, store the composite metrics in a hiearchical Python dataclass and post. For example, if you have an instance, `<metricdata>`, of a dataclass, `<MetricDataClass>`, to store, and an engine created via sqlalchemy, 
the code would look like:

```python
from daktylos.data_stores import SQLMetricStore

with SQLMetricStore(engine, create=True) as datastore:
    datastore.post(metric_name="TopLevelMetricName", metric_data=metricdata)
```
    
The post call can also take a dict of metadata, whose keys are of type str and whose value are of type str or int.  This 
metadata will be associated with the composite metric and can be recalled along with the metric data when queried.  It can 
also take a specific timestamp, which defaults to "now" if not specified.

The datastore object can also be used to retrieve data as an array of composite metrics, as array of each individual field of the composite (making it easier to use more readily in plotting), and each of these can be filtered against metadata fields that match specific values.  Some example calls:

```python
# latest 200 metrics as an array of instances of MetricDataClass:
datastore.dataclass_metrics_by_volume(metric_name="TopLevelMetricName", typ=MetricDataClass, count=200)
# latest 200 metrics as an array of daktylos's `<CompositeMetric>` class as a prommatic hierarhcy of data:
datastore.composite_metrics_by_volume(metric_name="TopLevelMetricName", count=200, metadata_filter={"platform": "Liunx"})
# metrics over the past 10 days, returned as a dictionary keyed on field name with value as an array of floats
datastore.metric_fields_by_date(metric_name="TopLevelMetricName", oldest=datatime.now() - datetime.timedelta(days=10))
```



                   
