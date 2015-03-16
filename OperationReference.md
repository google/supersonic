# Operation reference #
This wiki page contains a list of operations supported by Supersonic. Some quick usage information is conveyed together with customisation options.

# General notes #
A good understanding of operations is extremely important in order to use Supersonic efficiently. They are composed to create an operation tree which describes the data computation flow - from input sources, through actual data-manipulating operations (like hash-joining, sorting, various aggregations, etc.) and ending in data sinks which transfer rows to their final destination, be it on the disk or in an in-memory materialisation unit (`Table`).

If you're looking for information on expressions, which are tuple-level computations units, check out the [ExpressionReference](ExpressionReference.md).

**TODO:** This document is not complete and will, hopefully, gradually evolve towards completeness.

# Operations #

## Aggregation ##
Aggregation is a ubiquitous and extremely useful operation in virtually all areas of the database world. It allows us to obtain some "single-piece" information based on a sequence of values. Supersonic supports computation of various aggregation results listed below; their descriptions should be pretty self-explanatory:
  * `SUM`
  * `MIN`
  * `MAX`
  * `COUNT`
  * `CONCAT`
  * `FIRST`
  * `LAST`
Additional memory for the results will be allocated. It will grow as more and more results appear. It is also possible to start allocation with a specific block size. The number of rows in the result is guaranteed not to exceed the input row count.

To create the aggregation we use:
```
Operation* GroupAggregate(
    const SingleSourceProjector* group_by,
    const AggregationSpecification* aggregation,
    GroupAggregateOptions* options,
    Operation* child);
```

`group_by` - the source projector which specifies by which key we should perform the aggregation by

`aggregation` - a specification object providing details of the results  of the operation

`options` - lower-level memory management options; NULL argument will be substituted with a viable default

`child` - input operation

The `AggregationSpecification` object is initialised using a zero-argument constructor:
```
new AggregationSpecification()
```
> We can then use one of the `AddAggregation`-like methods to add information about desired outputs. All of them take an aggregation type (`MIN`, `COUNT`, etc.), data-input and result-output column names as their first three arguments. They are listed below.

```
// Creates an aggregation; output will have a default type.
AggregationSpecification* AddAggregation(
    Aggregation aggregation,
    const StringPiece& input_name,
    const StringPiece& output_name);
```

```
// Creates an aggregation which will apply the internal function to distinct values only. 
// For instance, DISTINCT COUNT(4, 5, 4) is 2.
AggregationSpecification*  AddDistinctAggregation(
    Aggregation aggregation,
    const StringPiece& input_name,
    const StringPiece& output_name);
```

```
// Creates an aggregation with a specified output type.
AggregationSpecification* AddAggregationWithDefinedOutputType(
    Aggregation aggregation,
    const StringPiece& input_name,
    const StringPiece& output_name,
    DataType output_type);
```

```
// A combination of the two above.
AggregationSpecification* AddDistinctAggregationWithDefinedOutputType(
    Aggregation aggregation,
    const StringPiece& input_name,
    const StringPiece& output_name,
    DataType output_type);
```

In most cases using a default `GroupAggregateOptions` object should be enough. For more control over memory management check out the object's API in supersonic/cursor/core/aggregate.h (link!).

## Compute ##
Compute is used to "promote" an expression to an operation - what it does is evaluating the given expression tree on the input operation's data. See [ExpressionReference](ExpressionReference.md) for more information on constructing expression trees.

```
Operation* Compute(const Expression* computation, Operation* child);
```

## Filter ##
Filters create predicate sieves which are evaluated for all rows taken from by the input cursor - only those rows for which the predicate holds are passed on.

```
Operation* Filter(const Expression* predicate,
                  const SingleSourceProjector* projector,
                  Operation* child);
```

`predicate` - the predicate expression used to filter rows

`projector` - projects the input columns creating a subset of columns which will be accepted by the predicate

`child` - input operation


## Generate ##
Generate creates a given number of placeholder rows of a zero-column tuple schema. This may not immediately strike you as something extremely useful, but the functionality combines naturally with Compute whenever we want to create rows containing constant, random or sequence values, as even if the computed expression does not need actual input, it has to know how many rows should be passed on. The API is as follows:

```
Operation* Generate(rowcount_t count);
```

`count` - number of created zero-column rows

## Limit ##
The Limit operation performs a simple, yet useful task of constraining the values returned by the input cursor to a specified number starting at a given offset. The rows at the beginning which are meant to be omitted are simply pulled from the underlying cursor and discarded.

The factory method for creating Limit operations is as follows:

```
Operation* Limit(rowcount_t offset,
                 rowcount_t limit,
                 Operation* child);
```
The arguments are quite self-explanatory.


## Sort ##
The idea of sorting itself requires little explanation. However, some overview of how sorting is carried out by Supersonic is in order.

Supersonic's Sort is **not stable**. However, this should not be an issue as there is support for performing several different-key sorts on the same data by supplying a proper `SortOrder` object. Sorting will be done by the first key column globally, then row ranges with matching first-sort keys will be identified and sorted by the second key. Rinse and repeat, until there's no more key columns or no more ranges.

`NULL`s are all considered equal to each other and lower than anything else, hence they will end up at the top for `ASCENDING` and at the bottom for `DESCENDING` sorts.

In-depth information on the complexity of sorting can be found in [sort.cc](link.md). The implementation uses STL sort for handling a single range, which is rather cache-friendly. Performance may vary based on the specified (soft) memory limit, as data will have to be written to disk once the limit is exceeded (more or less, explanation will follow).

There are as of now three sort operation factory methods described below.

```
Operation* Sort(
    const SortOrder* sort_order,
    const SingleSourceProjector* result_projector,
    size_t memory_limit,  // in bytes
    Operation* child);
```

```
// Temporary files will be stored in a directory with a given prefix.
Operation* SortWithTempDirPrefix(
    const SortOrder* sort_order,
    const SingleSourceProjector* result_projector,
    size_t memory_limit,
    StringPiece temporary_directory_prefix,
    Operation* child);
```

```
// Identical with Sort, but supports case insensitivity and limit on the
// number of returned elements. Also, unlike sort, its parameter is 
// serializable.
// Takes ownership of all input.
Operation* ExtendedSort(
    const ExtendedSortSpecification* specification,
    const SingleSourceProjector* result_projector,
    size_t memory_limit,
    Operation* child);
```

`sort_order` - an object which describes the column by which we want to sort. Specification is done using the `add` function
```
SortOrder* add(
    const SingleSourceProjector* projector,
    const ColumnOrder column_order);
```
where `column_order` is either `ASCENDING` or `DESCENDING`.

`result_projector` - projection which should be applied to the result. NULL will default to an identity projection.

`memory_limit` - a "soft" cap on the maximum memory that may be used by sorting. Soft means that the actual cap is in the same order of magnitude (shouldn't be higher than twice the specified value), but momentary (but not typical) use can still be higher.

`child` - input operation

`temporary_directory_prefix` - prefix for the temporary directory

`specification` - an `ExtendedSortSpecification` object described in [link! specification.proto] which allows the user to specify whether or not the sorting should be case-sensitive.