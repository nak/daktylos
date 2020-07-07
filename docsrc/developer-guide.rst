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

.. automodule:: daktylos.data

*daktylos* is designed around capturing a set of related metrics in a hiearchy.  Composite metrics can be created
in a leaf-branch style architecture, and child metrics can be referenced down through a hierarchy based on a
dict-like interface.

Creating Metrics
================
Metric instances can be created as a core metric -- a single key/value pair -- via the `Metric` class, or a
hierarchical composite of metrics via the `CompositeMetric` class.  Both of these derive from an abstract
`BasicMetric` class


.. autoclass:: daktylos.data.BasicMetric

.. autoclass:: daktylos.data.Metric

.. autoclass:: daktylos.data.CompositeMetric

Storing Metrics
===============

.. autoclass:: daktylos.data.MetricStore

SQL Implementation
------------------

.. autoclass:: daktylos.data_stores.sql.SQLMetricStore

Metrics Validation
==================

.. automodule:: daktylos.rules.engine

.. autoclass:: daktylos.rules.engine.Rule

.. autoclass:: daktylos.rules.engine.RulesEngine

.. autoclass:: daktylos.rules.status.ValidationStatus

