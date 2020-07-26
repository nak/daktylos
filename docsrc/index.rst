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

Introduction
============

Welcome to *daktylos*, a simple framework for storing and retrieving hierarchical metric values.  The insipration
for this came from a need to capture code quality and performance metrics, such as code coverage data or startup times
for an app, in a simple, performant manner, and apply validation rules (copmaring against a threshold value) to them.

The name Daktylos comes from an ancient Greek unit of measurement, one that is based on the length of a finger.  Since
the initial need to create *daktylos* comes from capturing metrics that provide insights to the developer that inform
him or her about direct measurements of the code written, this seemed a fitting name.

Creating Metrics
================

*daktylos* is designed around capturing a set of related metrics in a hierarchy.  Composite metrics can be created
in a leaf-branch style architecture, and child metrics can be referenced down through a hierarchy based on a
dict-like interface.

.. automodule:: daktylos.data
    :members: Metric, CompositeMetric

Metrics Storage
===============

*daktylos* provides the ability to store, retrieve and purge metrics from a relational database.  It also probides
the proper abstraction for data access:

.. autoclass:: daktylos.data.MetricStore

with the specific implementation via SQL and `SqlAlchemy <https://>` provided through the *daktylos.data_stores.sql`
module:

.. autoclass:: daktylos.data_stores.sql.SQLMetricStore

Applying Rules to Metrics
=========================

One of the primary features of *daktylos* is the ability to easily apply threshold rules to metrics.  The client
can validate a given set of metric values against user-defined rules to produce alerts or failures when they
values are out of expected range.

.. automodule:: daktylos.rules.status
    :members: ValidationStatus