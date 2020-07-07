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

.. automodule:: daktylos.data

*daktylos* is designed around capturing a set of related metrics in a hiearchy.  Composite metrics can be created
in a leaf-branch style architecture, and child metrics can be referenced down through a hierarchy based on a
dict-like interface.

Composite Metrics
=================

.. autoclass:: daktylos.data.CompositeMetric

