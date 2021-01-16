Welcome to *daktylos*'s documentation!
=========================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   developer-guide


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Introduction/class
==================

Welcome to *daktylos*, a simple framework for storing and retrieving hierarchical metric values.  The inspiration
for this came from a need to capture code quality and performance metrics, such as code coverage data or startup times
for an app, in a simple, performant manner, and apply validation rules (comparing against a threshold value) to them.

The name Daktylos comes from an ancient Greek unit of measurement, one that is based on the length of a finger.  Since
the initial need to create *daktylos* comes from capturing metrics that provide insights to the developer that inform
him or her about direct measurements of the code written, this seemed a fitting name.

Creating Metrics
================

*daktylos* is designed around capturing a set of related metrics in a hierarchy.  Composite metrics that are
consumable by *daktylos* can be created in two ways: using data classes or constructing explicit composite metrics
programatically.  In general, using dataclasses is simpler.  However, if you expect frequent changes (additions,
deletions, renaming of elements) over time, dataclasses can start to get messy and even problematic.  In such
as case, the programmatic approach is advisable.

Creation through Dataclasses
----------------------------

Generally speaking, as long as a dataclass follows some basic rules, they can be used as is without any need
for special code.  It is adviable, however, to inherit your data class from `daktylos.data.MetricDataClass` as
this will ensure the rules are enforced when using a tool like mypy, at least in principle.

.. autoclass:: daktylos.data.MetricDataClass


Programmatic Creation
---------------------
Composite metrics cstr contructed programatically in a leaf-branch style architecture.


.. automodule:: daktylos.data
    :members: CompositeMetric, Metric

Metrics Storage
===============

*daktylos* provides the ability to store, retrieve and purge metrics from a relational database.  It also probides
the proper abstraction for data access.  A specific implementation via SQL and
`_SqlAlchemy <https://www.sqlalchemy.org/>` is provided.

.. automodule:: daktylos.data
    :members: MetricStore


.. automodule:: daktylos.data_stores.sql
    :members: SQLMetricStore

Applying Rules to Metrics
=========================

One of the primary features of *daktylos* is the ability to easily apply threshold rules to metrics.  The client
can validate a given set of metric values against user-defined rules to produce alerts or failures when they
values are out of expected range.

.. automodule:: daktylos.rules.status
    :members: ValidationStatus
