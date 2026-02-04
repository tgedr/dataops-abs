# tgedr-dataops-abs

![Coverage](./coverage.svg)
[![PyPI](https://img.shields.io/pypi/v/tgedr-dataops-abs)](https://pypi.org/project/tgedr-dataops-abs/)


data operations related code - abstractions


## motivation
abstract constructs to depict commonly use cases in data engineering context. Think about commonly accepted abstract classes and interfaces that can be implemented and extended according to different requirements and constraints.


## package namespaces and its contents

- __Chain__ : chain-like abstract class (for sequential processing) ([example](tests/tgedr_dataops_abs/test_processor_chain.py))
- __Etl__ : Extract-Transform-Load abstract class to be extended and used in data pipelines ([example](tests/tgedr_dataops_abs/test_etl.py))
- __Great_Expectations_Validation__ : data validation abstract class to be extended and to validate against json-defined expectations as consumend by the great expectations library ([example](tests/tgedr_dataops_abs/test_great_expectations_validation.py))
- __Processor__ : abstract class for data processing ([example](tests/tgedr_dataops_abs/test_processor_chain.py))
- __Sink__: abstract **sink** class defining methods (`put`and `delete`) to manage persistence of data somewhere as defined by implementing classes  ([example](tests/tgedr_dataops_abs/test_sink.py))
- __Source__: abstract **source** class defining methods (`list` and `get`) to manage retrieval of data from somewhere as defined by implementing classes ([example](tests/tgedr_dataops_abs/test_source.py))
- __Store__ : abstract class used to manage persistence, defining CRUD-like (CreateReadUpdateDelete) methods ([example](tests/tgedr_dataops_abs/test_store.py))



## development
- main requirements:
  - _uv_  
  - _bash_
- Clone the repository like this:

  ``` bash
  git clone git@github.com:tgedr/dataops-abs
  ```
- cd into the folder: `cd dataops-abs`
- install requirements: `./helper.sh reqs`
