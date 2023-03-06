<h1 align="center">Utility Functions</h1>

## Overview
This repository contains a collection of utility functions that can be used in various use cases to perform common tasks. The functions are flexible to apply to various datasets in multiple use cases.

## Folder Structure
Each folder is catgorized, and follows a similar structure below.
```
├── DataAggregation/
│   ├── functions.py - Module of functions
│   ├── tests.py - Tests defined for each function
│   └── main.py - Import module and execute function tests
├── DataCleansing/
├── DataPerformanceMetrics/
├── DataQuery/
├── DataTextAnalysis/
├── DataVisualization/
├── GPSUtilities/ - Functions for working with GPS data
├── IOUtilities/ - Functions related to input/output
├── Other/ - Some of these might be able to move to other categories
└── RFunctions/
```

## Usage
These functions were created in Jupyter notebooks using Azure Databricks. To use these functions, you can clone the repository and import the notebooks.

```git clone https://github.com/ahasan-suncor/EAUtilityFunctions.git```

## Tests
These functions were tested using the ```unittest``` testing framework. Each function has its own set of tests. The tests are organized into classes for each function and separate methods for each test.
