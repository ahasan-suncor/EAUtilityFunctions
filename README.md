<h1 align="center">Utility Functions</h1>

## Overview
This repository contains a collection of utility functions that can be used in various use cases to perform common tasks. The functions are flexible to apply to various datasets in multiple use cases.

## Folder Structure
Each function is categorized, and follows a similar structure below.
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
├── RFunctions/
│   ├──DataCleansing/
└── TestData/ - Data used to test some of the functions
```

## Usage
These functions were created in Jupyter notebooks using Azure Databricks. To use these functions, you can clone the repository and import the notebooks.

```git clone https://github.com/ahasan-suncor/EAUtilityFunctions.git```

Every function is documented (ex. docstring) that explains its purpose and shows the inputs and outputs. To see how the function is called, refer to its tests.

## Tests
The Python functions were tested using the ```unittest``` testing framework. Each function has its own set of tests.
Every function has a test class, and separate methods for each test.

The R functions were tested using the ```testthat``` library.

## Naming Conventions
- snake_case for function/method names, variables, and file names
- PascalCase for class names, and directory names
- Function names are verbs
- Class names are nouns
  - Class test names are in the format `<NameOfFunction>Tests`
  - The test method name is in the format `test_<name_of_function>_<test_description>`
