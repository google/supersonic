# Supersonic Query Engine #
## Introduction ##
Supersonic is an **ultra-fast**, **column oriented** query engine library written in C++. It provides a set of data transformation primitives which make heavy use of cache-aware algorithms, SIMD instructions and vectorised execution, allowing it to exploit the capabilities and resources of modern, hyper pipelined CPUs. It is designed to work in a single process.

Supersonic is intended to be used as a back-end for various data warehousing projects. The functionalities it provides are:
  * **speed**
    * cache consciousness
    * instruction pipelining
    * SIMD use
    * efficient memory allocation
    * custom data structures

  * **reliability**
    * failure handling
    * high test coverage

  * **robustness**
    * support for standard columnar database operations
    * a wide range of specialised expressions (including many math, string and date manipulation functionalities)

## Get Supersonic ##
The tarball is available in the [Downloads](http://code.google.com/p/supersonic/downloads/list) section. For frequent updates it might be more convenient to sync up with the Git repo as an anonymous user:
```
git clone https://code.google.com/p/supersonic/
```

Information for developers resides [here](DevGuide.md).

## Where to start? ##
Installation instructions are available in the top directory [INSTALL](https://code.google.com/p/supersonic/source/browse/INSTALL) file. There are dependencies on several other libraries which are all listed therein. Supersonic's aim is to be fairly portable, but it has so far only been tested on Linux systems.

## Documentation ##
After installation visit [guide tests](https://code.google.com/p/supersonic/source/browse/test/guide/) for a few use cases with extensive comments and guidelines on how to use the API.

Some documentation is available on the wiki - it is currently under development.

There is also a [presentation](http://code.google.com/p/supersonic/downloads/detail?name=api-presentation.pdf&can=2&q=).

## Discussion ##
Supersonic has a [mailing list](http://groups.google.com/group/supersonic-query-engine).