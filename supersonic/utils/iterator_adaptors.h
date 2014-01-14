// Copyright 2005 Google Inc. All Rights Reserved.
//
// This file defines some iterator adapters for working on:
// - containers where the value_type is pair<>, such as either
//   std::unordered_map<K,V>, or list<pair<>>.
// - containers where the value_type is a pointer like type.
// - containers that you need to iterate backwards.
//
#ifndef UTIL_GTL_ITERATOR_ADAPTORS_H_
#define UTIL_GTL_ITERATOR_ADAPTORS_H_

#include <iterator>
#include "supersonic/utils/std_namespace.h"

#include "supersonic/utils/template_util.h"
#include <type_traits>
#include "supersonic/utils/std_namespace.h"   // For remove_pointer

namespace util {
namespace gtl {

// util::gtl::adaptor_helper is a helper class for creating iterator adaptors.
// util::gtl::adaptor_helper uses template metaprogramming to define value_type,
// pointer, and reference.
//
// We use template metaprogramming to make a compile-time determination of
// const-ness.  To make this determination, we compare whether the reference
// type of the parameterized iterator is defined as either "value_type&" or
// "const value_type&".  When we have the const version, the templates
// detect that the reference parameter is not equal to value_type&.
template<typename It, typename Val>
struct adaptor_helper {
  typedef Val                                             value_type;
  typedef typename base::if_<
      base::type_equals_<typename std::iterator_traits<It>::reference,
                         typename std::iterator_traits<It>::value_type&>::value,
      Val*, const Val*>::type pointer;
  typedef typename base::if_<
      base::type_equals_<typename std::iterator_traits<It>::reference,
                         typename std::iterator_traits<It>::value_type&>::value,
      Val&, const Val&>::type reference;
  typedef typename std::iterator_traits<It>::difference_type difference_type;
  typedef typename std::iterator_traits<It>::iterator_category
      iterator_category;
};

// ptr_adaptor_helper is similar to adaptor_helper, but the second argument is a
// pointer type and our resulting value_type is the pointed-to type.
//
// We don't need to worry about const-ness here because the caller only
// interacts with dereferenced values, never with references or pointers to the
// original pointer values.
template<typename It, typename PtrVal>
struct ptr_adaptor_helper {
  typedef typename base::remove_pointer<PtrVal>::type     value_type;
  typedef const PtrVal&                                   pointer;
  typedef typename base::remove_pointer<PtrVal>::type&    reference;
  typedef typename std::iterator_traits<It>::difference_type difference_type;
  typedef typename std::iterator_traits<It>::iterator_category
      iterator_category;
};

}  // namespace gtl
}  // namespace util

// In both iterator adaptors, iterator_first<> and iterator_second<>,
// we build a new iterator based on a parameterized iterator type, "It".
// The value type, "_Vt" is determined by "It::value_type::first" or
// "It::value_type::second", respectively.

// iterator_first<> adapts an iterator to return the first value of a pair.
// It is equivalent to calling it->first on every value.
// Example:
//
// std::unordered_map<string, int> values;
// values["foo"] = 1;
// values["bar"] = 2;
// for (iterator_first<std::unordered_map<string, int>::iterator> x = values.begin();
//      x != values.end(); ++x) {
//   printf("%s", x->c_str());
// }
template <typename It, typename Val = typename std::iterator_traits<
                           It>::value_type::first_type>
class iterator_first {
 private:
  // Helper template to define our necessary typedefs.
  typedef typename util::gtl::adaptor_helper<It, Val> helper;

  // The internal iterator.
  It it_;

 public:
  typedef iterator_first<It, Val>                     iterator;
  typedef typename helper::iterator_category          iterator_category;
  typedef typename helper::value_type                 value_type;
  typedef typename helper::pointer                    pointer;
  typedef typename helper::reference                  reference;
  typedef typename helper::difference_type            difference_type;

  iterator_first() : it_() {}
  iterator_first(const It& it) : it_(it) {}  // NOLINT(runtime/explicit)

  // Allow "upcasting" from iterator_first<T*const*> to
  // iterator_first<const T*const*>.
  template<typename OtherIt>
  iterator_first(const iterator_first<OtherIt, Val>& other)
      : it_(other.base()) {}

  // Provide access to the wrapped iterator.
  const It& base() const { return it_; }

  reference operator*() const { return it_->first; }
  pointer   operator->() const { return &(operator*()); }

  iterator& operator++() { it_++; return *this; }
  iterator  operator++(int /*unused*/) { return iterator(it_++); }
  iterator& operator--() { it_--; return *this; }
  iterator  operator--(int /*unused*/) { return iterator(it_--); }

  iterator& operator+=(const difference_type& d) { it_ += d; return *this; }
  iterator& operator-=(const difference_type& d) { it_ -= d; return *this; }
  iterator operator+(const difference_type& d) const { return it_ + d; }
  iterator operator-(const difference_type& d) const { return it_ - d; }
  friend iterator operator+(const difference_type& d, const iterator& it) {
    return it.it_ + d;
  }
  difference_type operator-(const iterator& x) const { return it_ - x.it_; }
  reference operator[](const difference_type& d) const { return it_[d].first; }

  friend bool operator<(const iterator& a, const iterator& b) {
    return a.it_ < b.it_;
  }
  friend bool operator>(const iterator& a, const iterator& b) {
    return a.it_ > b.it_;
  }
  friend bool operator<=(const iterator& a, const iterator& b) {
    return a.it_ <= b.it_;
  }
  friend bool operator>=(const iterator& a, const iterator& b) {
    return a.it_ >= b.it_;
  }

  bool operator==(const iterator& x) const { return it_ == x.it_; }
  bool operator!=(const iterator& x) const { return it_ != x.it_; }

  // Convenience operators to allow comparison against a
  // native iterator, for example, when using it != container.end();
  bool operator==(const It& x) const      { return it_ == x; }
  bool operator!=(const It& x) const      { return it_ != x; }
};

template<typename It>
inline iterator_first<It> make_iterator_first(const It& it) {
  return iterator_first<It>(it);
}

// iterator_second<> adapts an iterator to return the second value of a pair.
// It is equivalent to calling it->second on every value.
// Example:
//
// std::unordered_map<string, int> values;
// values["foo"] = 1;
// values["bar"] = 2;
// for (iterator_second<std::unordered_map<string, int>::iterator> x = values.begin();
//      x != values.end(); ++x) {
//   int v = *x;
//   printf("%d", v);
// }
template <typename It, typename Val = typename std::iterator_traits<
                           It>::value_type::second_type>
class iterator_second {
 private:
  // Helper template to define our necessary typedefs.
  typedef typename util::gtl::adaptor_helper<It, Val> helper;

  // The internal iterator.
  It it_;

 public:
  typedef iterator_second<It, Val>                    iterator;
  typedef typename helper::iterator_category          iterator_category;
  typedef typename helper::value_type                 value_type;
  typedef typename helper::pointer                    pointer;
  typedef typename helper::reference                  reference;
  typedef typename helper::difference_type            difference_type;

  iterator_second() : it_() {}
  iterator_second(const It& it) : it_(it) {}  // NOLINT(runtime/explicit)

  // Allow "upcasting" from iterator_second<T*const*> to
  // iterator_second<const T*const*>.
  template<typename OtherIt>
  iterator_second(const iterator_second<OtherIt, Val>& other)
      : it_(other.base()) {}

  // Provide access to the wrapped iterator.
  const It& base() const { return it_; }

  reference operator*() const { return it_->second; }
  pointer   operator->() const { return &(operator*()); }

  iterator& operator++() { it_++; return *this; }
  iterator  operator++(int /*unused*/) { return iterator(it_++); }
  iterator& operator--() { it_--; return *this; }
  iterator  operator--(int /*unused*/) { return iterator(it_--); }

  iterator& operator+=(const difference_type& d) { it_ += d; return *this; }
  iterator& operator-=(const difference_type& d) { it_ -= d; return *this; }
  iterator operator+(const difference_type& d) const { return it_ + d; }
  iterator operator-(const difference_type& d) const { return it_ - d; }
  friend iterator operator+(const difference_type& d, const iterator& it) {
    return it.it_ + d;
  }
  difference_type operator-(const iterator& x) const { return it_ - x.it_; }
  reference operator[](const difference_type& d) const { return it_[d].second;}

  friend bool operator<(const iterator& a, const iterator& b) {
    return a.it_ < b.it_;
  }
  friend bool operator>(const iterator& a, const iterator& b) {
    return a.it_ > b.it_;
  }
  friend bool operator<=(const iterator& a, const iterator& b) {
    return a.it_ <= b.it_;
  }
  friend bool operator>=(const iterator& a, const iterator& b) {
    return a.it_ >= b.it_;
  }

  bool operator==(const iterator& x) const { return it_ == x.it_; }
  bool operator!=(const iterator& x) const { return it_ != x.it_; }

  // Convenience operators to allow comparison against a
  // native iterator, for example, when using it != container.end();
  bool operator==(const It& x) const      { return it_ == x; }
  bool operator!=(const It& x) const      { return it_ != x; }
};

// Helper function to construct an iterator.
template<typename It>
inline iterator_second<It> make_iterator_second(const It& it) {
  return iterator_second<It>(it);
}

// iterator_second_ptr<> adapts an iterator to return the dereferenced second
// value of a pair.
// It is equivalent to calling *it->second on every value.
// The same result can be achieved by composition
// iterator_ptr<iterator_second<> >
// Can be used with maps where values are regular pointers or pointers wrapped
// into linked_ptr. This iterator adaptor can be used by classes to give their
// clients access to some of their internal data without exposing too much of
// it.
//
// Example:
// class MyClass {
//  public:
//   MyClass(const string& s);
//   string DebugString() const;
// };
// typedef std::unordered_map<string, linked_ptr<MyClass> > MyMap;
// typedef iterator_second_ptr<MyMap::iterator> MyMapValuesIterator;
// MyMap values;
// values["foo"].reset(new MyClass("foo"));
// values["bar"].reset(new MyClass("bar"));
// for (MyMapValuesIterator it = values.begin(); it != values.end(); ++it) {
//   printf("%s", it->DebugString().c_str());
// }
template <typename It, typename _PtrVal = typename std::iterator_traits<
                           It>::value_type::second_type>
class iterator_second_ptr {
 private:
  // Helper template to define our necessary typedefs.
  typedef typename util::gtl::ptr_adaptor_helper<It, _PtrVal> helper;

  // The internal iterator.
  It it_;

 public:
  typedef iterator_second_ptr<It, _PtrVal>            iterator;
  typedef typename helper::iterator_category          iterator_category;
  typedef typename helper::value_type                 value_type;
  typedef typename helper::pointer                    pointer;
  typedef typename helper::reference                  reference;
  typedef typename helper::difference_type            difference_type;

  iterator_second_ptr() : it_() {}
  iterator_second_ptr(const It& it) : it_(it) {}  // NOLINT(runtime/explicit)

  // Allow "upcasting" from iterator_second_ptr<T*const*> to
  // iterator_second_ptr<const T*const*>.
  template<typename OtherIt>
  iterator_second_ptr(const iterator_second_ptr<OtherIt, _PtrVal>& other)
      : it_(other.base()) {}

  // Provide access to the wrapped iterator.
  const It& base() const { return it_; }

  reference operator*() const { return *it_->second; }
  pointer   operator->() const { return it_->second; }

  iterator& operator++() { ++it_; return *this; }
  iterator  operator++(int /*unused*/) { return iterator(it_++); }
  iterator& operator--() { --it_; return *this; }
  iterator  operator--(int /*unused*/) { return iterator(it_--); }

  iterator& operator+=(const difference_type& d) { it_ += d; return *this; }
  iterator& operator-=(const difference_type& d) { it_ -= d; return *this; }
  iterator operator+(const difference_type& d) const { return it_ + d; }
  iterator operator-(const difference_type& d) const { return it_ - d; }
  friend iterator operator+(const difference_type& d, const iterator& it) {
    return it.it_ + d;
  }
  difference_type operator-(const iterator& x) const { return it_ - x.it_; }
  reference operator[](const difference_type& d) const {
    return *(it_[d].second);
  }

  friend bool operator<(const iterator& a, const iterator& b) {
    return a.it_ < b.it_;
  }
  friend bool operator>(const iterator& a, const iterator& b) {
    return a.it_ > b.it_;
  }
  friend bool operator<=(const iterator& a, const iterator& b) {
    return a.it_ <= b.it_;
  }
  friend bool operator>=(const iterator& a, const iterator& b) {
    return a.it_ >= b.it_;
  }

  bool operator==(const iterator& x) const { return it_ == x.it_; }
  bool operator!=(const iterator& x) const { return it_ != x.it_; }

  // Convenience operators to allow comparison against a
  // native iterator, for example, when using it != container.end();
  bool operator==(const It& x) const      { return it_ == x; }
  bool operator!=(const It& x) const      { return it_ != x; }
};

// Helper function to construct an iterator.
template<typename It>
inline iterator_second_ptr<It> make_iterator_second_ptr(const It& it) {
  return iterator_second_ptr<It>(it);
}

// iterator_ptr<> adapts an iterator to return the dereferenced value.
// With this adaptor you can write *it instead of **it, or it->something instead
// of (*it)->something.
// Can be used with vectors and lists where values are regular pointers
// or pointers wrapped into linked_ptr. This iterator adaptor can be used by
// classes to give their clients access to some of their internal data without
// exposing too much of it.
//
// Example:
// class MyClass {
//  public:
//   MyClass(const string& s);
//   string DebugString() const;
// };
// typedef vector<linked_ptr<MyClass> > MyVector;
// typedef iterator_ptr<MyVector::iterator> DereferencingIterator;
// MyVector values;
// values.push_back(make_linked_ptr(new MyClass("foo")));
// values.push_back(make_linked_ptr(new MyClass("bar")));
// for (DereferencingIterator it = values.begin(); it != values.end(); ++it) {
//   printf("%s", it->DebugString().c_str());
// }
//
// Without iterator_ptr you would have to do (*it)->DebugString()
template <typename It,
          typename _PtrVal = typename std::iterator_traits<It>::value_type>
class iterator_ptr {
 private:
  // Helper template to define our necessary typedefs.
  typedef typename util::gtl::ptr_adaptor_helper<It, _PtrVal> helper;

  // The internal iterator.
  It it_;

 public:
  typedef iterator_ptr<It, _PtrVal>                   iterator;
  typedef typename helper::iterator_category          iterator_category;
  typedef typename helper::value_type                 value_type;
  typedef typename helper::pointer                    pointer;
  typedef typename helper::reference                  reference;
  typedef typename helper::difference_type            difference_type;

  iterator_ptr() : it_() {}
  iterator_ptr(const It& it) : it_(it) {}  // NOLINT(runtime/explicit)

  // Allow "upcasting" from iterator_ptr<T*const*> to
  // iterator_ptr<const T*const*>.
  template<typename OtherIt>
  iterator_ptr(const iterator_ptr<OtherIt, _PtrVal>& other)
      : it_(other.base()) {}

  // Provide access to the wrapped iterator.
  const It& base() const { return it_; }

  reference operator*() const { return **it_; }
  pointer   operator->() const { return *it_; }

  iterator& operator++() { ++it_; return *this; }
  iterator  operator++(int /*unused*/) { return iterator(it_++); }
  iterator& operator--() { --it_; return *this; }
  iterator  operator--(int /*unused*/) { return iterator(it_--); }

  iterator& operator+=(const difference_type& d) { it_ += d; return *this; }
  iterator& operator-=(const difference_type& d) { it_ -= d; return *this; }
  iterator operator+(const difference_type& d) const { return it_ + d; }
  iterator operator-(const difference_type& d) const { return it_ - d; }
  friend iterator operator+(const difference_type& d, const iterator& it) {
    return it.it_ + d;
  }
  difference_type operator-(const iterator& x) const { return it_ - x.it_; }
  reference operator[](const difference_type& d) const { return *(it_[d]); }

  friend bool operator<(const iterator& a, const iterator& b) {
    return a.it_ < b.it_;
  }
  friend bool operator>(const iterator& a, const iterator& b) {
    return a.it_ > b.it_;
  }
  friend bool operator<=(const iterator& a, const iterator& b) {
    return a.it_ <= b.it_;
  }
  friend bool operator>=(const iterator& a, const iterator& b) {
    return a.it_ >= b.it_;
  }

  bool operator==(const iterator& x) const { return it_ == x.it_; }
  bool operator!=(const iterator& x) const { return it_ != x.it_; }

  // Convenience operators to allow comparison against a
  // native iterator, for example, when using it != container.end();
  bool operator==(const It& x) const      { return it_ == x; }
  bool operator!=(const It& x) const      { return it_ != x; }
};

// Helper function to construct an iterator.
template<typename It>
inline iterator_ptr<It> make_iterator_ptr(const It& it) {
  return iterator_ptr<It>(it);
}

namespace util {
namespace gtl {
namespace internal {

// Template that uses SFINAE to inspect Container abilities:
// . Set has_size_type true, iff T::size_type is defined
// . Define size_type as T::size_type if defined, or size_t otherwise
template<typename C>
struct container_traits {
 private:
  // Provide Yes and No to make the SFINAE tests clearer.
  typedef base::small_  Yes;
  typedef base::big_    No;

  // Test for availability of C::size_typae.
  template<typename U>
  static Yes test_size_type(typename U::size_type*);
  template<typename>
  static No test_size_type(...);

  // Conditional provisioning of a size_type which defaults to size_t.
  template<bool Cond, typename U = void>
  struct size_type_def {
    typedef typename U::size_type type;
  };
  template<typename U>
  struct size_type_def<false, U> {
    typedef size_t type;
  };

 public:
  // Determine whether C::size_type is available.
  static const bool has_size_type = sizeof(test_size_type<C>(0)) == sizeof(Yes);

  // Provide size_type as either C::size_type if available, or as size_t.
  typedef typename size_type_def<has_size_type, C>::type size_type;
};

template<typename C>
struct IterGenerator {
  typedef C container_type;
  typedef typename C::iterator iterator;
  typedef typename C::const_iterator const_iterator;

  static iterator begin(container_type& c) {  // NOLINT(runtime/references)
    return c.begin();
  }
  static iterator end(container_type& c) {  // NOLINT(runtime/references)
    return c.end();
  }
  static const_iterator begin(const container_type& c) { return c.begin(); }
  static const_iterator end(const container_type& c) { return c.end(); }
};

template<typename SubIterGenerator>
struct ReversingIterGeneratorAdaptor {
  typedef typename SubIterGenerator::container_type container_type;
  typedef std::reverse_iterator<typename SubIterGenerator::iterator> iterator;
  typedef std::reverse_iterator<typename SubIterGenerator::const_iterator>
      const_iterator;

  static iterator begin(container_type& c) {  // NOLINT(runtime/references)
    return iterator(SubIterGenerator::end(c));
  }
  static iterator end(container_type& c) {  // NOLINT(runtime/references)
    return iterator(SubIterGenerator::begin(c));
  }
  static const_iterator begin(const container_type& c) {
    return const_iterator(SubIterGenerator::end(c));
  }
  static const_iterator end(const container_type& c) {
    return const_iterator(SubIterGenerator::begin(c));
  }
};


// C:             the container type
// Iter:          the type of mutable iterator to generate
// ConstIter:     the type of constant iterator to generate
// IterGenerator: a policy type that returns native iterators from a C
template<typename C, typename Iter, typename ConstIter,
         typename IterGenerator = util::gtl::internal::IterGenerator<C> >
class iterator_view_helper {
 public:
  typedef C container_type;
  typedef Iter iterator;
  typedef ConstIter const_iterator;
  typedef typename std::iterator_traits<iterator>::value_type value_type;
  typedef typename util::gtl::internal::container_traits<C>::size_type
      size_type;

  explicit iterator_view_helper(
      container_type& c)  // NOLINT(runtime/references)
      : c_(&c) {
  }

  iterator begin() { return iterator(IterGenerator::begin(container())); }
  iterator end() { return iterator(IterGenerator::end(container())); }
  const_iterator begin() const {
    return const_iterator(IterGenerator::begin(container()));
  }
  const_iterator end() const {
    return const_iterator(IterGenerator::end(container()));
  }
  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }
  const container_type& container() const { return *c_; }
  container_type& container() { return *c_; }

  bool empty() const { return begin() == end(); }
  size_type size() const { return c_->size(); }

 private:
  // TODO(user): Investigate making ownership be via IterGenerator.
  container_type* c_;
};

// TODO(user): Investigate unifying const_iterator_view_helper
// with iterator_view_helper.
template<typename C, typename ConstIter,
         typename IterGenerator = util::gtl::internal::IterGenerator<C> >
class const_iterator_view_helper {
 public:
  typedef C container_type;
  typedef ConstIter const_iterator;
  typedef typename std::iterator_traits<const_iterator>::value_type value_type;
  typedef typename util::gtl::internal::container_traits<C>::size_type
      size_type;

  explicit const_iterator_view_helper(const container_type& c) : c_(&c) { }

  // Allow implicit conversion from the corresponding iterator_view_helper.
  // Erring on the side of constness should be allowed. E.g.:
  //    MyMap m;
  //    key_view_type<MyMap>::type keys = key_view(m);  // ok
  //    key_view_type<const MyMap>::type const_keys = key_view(m);  // ok
  template<typename Iter>
  const_iterator_view_helper(
      const iterator_view_helper<container_type, Iter, const_iterator,
                                 IterGenerator>& v)
      : c_(&v.container()) { }

  const_iterator begin() const {
    return const_iterator(IterGenerator::begin(container()));
  }
  const_iterator end() const {
    return const_iterator(IterGenerator::end(container()));
  }
  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }
  const container_type& container() const { return *c_; }

  bool empty() const { return begin() == end(); }
  size_type size() const { return c_->size(); }

 private:
  const container_type* c_;
};

}  // namespace internal
}  // namespace gtl
}  // namespace util

// Traits to provide a typedef abstraction for the return value
// of the key_view() and value_view() functions, such that
// they can be declared as:
//
//    template <typename C>
//    typename key_view_type<C>::type key_view(C& c);
//
//    template <typename C>
//    typename value_view_type<C>::type value_view(C& c);
//
// This abstraction allows callers of these functions to use readable
// type names, and allows the maintainers of iterator_adaptors.h to
// change the return types if needed without updating callers.

template<typename C>
struct key_view_type {
  typedef util::gtl::internal::iterator_view_helper<
      C,
      iterator_first<typename C::iterator>,
      iterator_first<typename C::const_iterator> > type;
};

template<typename C>
struct key_view_type<const C> {
  typedef util::gtl::internal::const_iterator_view_helper<
      C,
      iterator_first<typename C::const_iterator> > type;
};

template<typename C>
struct value_view_type {
  typedef util::gtl::internal::iterator_view_helper<
      C,
      iterator_second<typename C::iterator>,
      iterator_second<typename C::const_iterator> > type;
};

template<typename C>
struct value_view_type<const C> {
  typedef util::gtl::internal::const_iterator_view_helper<
      C,
      iterator_second<typename C::const_iterator> > type;
};

// The key_view and value_view functions provide pretty ways to iterate either
// the keys or the values of a map using range based for loops.
//
// Example:
//    std::unordered_map<int, string> my_map;
//    ...
//    for (string val : value_view(my_map)) {
//      ...
//    }

template<typename C>
typename key_view_type<C>::type
key_view(C& map) {  // NOLINT(runtime/references)
  return typename key_view_type<C>::type(map);
}

template<typename C>
typename key_view_type<const C>::type key_view(const C& map) {
  return typename key_view_type<const C>::type(map);
}

template<typename C>
typename value_view_type<C>::type
value_view(C& map) {  // NOLINT(runtime/references)
  return typename value_view_type<C>::type(map);
}

template<typename C>
typename value_view_type<const C>::type value_view(const C& map) {
  return typename value_view_type<const C>::type(map);
}

namespace util {
namespace gtl {

// Abstract container view that dereferences pointer elements.
//
// Example:
//   vector<string*> elements;
//   for (const string& element : deref_view(elements)) {
//     ...
//   }
//
// Note: If you pass a temporary container to deref_view, be careful that the
// temporary container outlives the deref_view to avoid dangling references.
// This is fine:  PublishAll(deref_view(Make());
// This is not:   for (const auto& v : deref_view(Make())) { Publish(v); }

template<typename C>
struct deref_view_type {
  typedef internal::iterator_view_helper<
      C,
      iterator_ptr<typename C::iterator>,
      iterator_ptr<typename C::const_iterator> > type;
};

template<typename C>
struct deref_view_type<const C> {
  typedef internal::const_iterator_view_helper<
      C,
      iterator_ptr<typename C::const_iterator> > type;
};

template<typename C>
typename deref_view_type<C>::type
deref_view(C& map) {  // NOLINT(runtime/references)
  return typename deref_view_type<C>::type(map);
}

template<typename C>
typename deref_view_type<const C>::type deref_view(const C& map) {
  return typename deref_view_type<const C>::type(map);
}

// Abstract container view that iterates backwards.
//
// Example:
//   vector<string> elements;
//   for (const string& element : reversed_view(elements)) {
//     ...
//   }
//
// Note: If you pass a temporary container to reversed_view_type, be careful
// that the temporary container outlives the reversed_view to avoid dangling
// references. This is fine:  PublishAll(reversed_view(Make());
// This is not:   for (const auto& v : reversed_view(Make())) { Publish(v); }

template<typename C>
struct reversed_view_type {
 private:
  typedef internal::ReversingIterGeneratorAdaptor<
      internal::IterGenerator<C> > policy;

 public:
  typedef internal::iterator_view_helper<
      C,
      typename policy::iterator,
      typename policy::const_iterator,
      policy> type;
};

template<typename C>
struct reversed_view_type<const C> {
 private:
  typedef internal::ReversingIterGeneratorAdaptor<
      internal::IterGenerator<C> > policy;

 public:
  typedef internal::const_iterator_view_helper<
     C,
     typename policy::const_iterator,
     policy> type;
};

template<typename C>
typename reversed_view_type<C>::type
reversed_view(C& c) {  // NOLINT(runtime/references)
  return typename reversed_view_type<C>::type(c);
}

template<typename C>
typename reversed_view_type<const C>::type reversed_view(const C& c) {
  return typename reversed_view_type<const C>::type(c);
}

}  // namespace gtl
}  // namespace util

#endif  // UTIL_GTL_ITERATOR_ADAPTORS_H_
