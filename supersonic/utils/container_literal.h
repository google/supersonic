// Copyright 2009 Google Inc. All Rights Reserved.

// Utilities for simulating container literals in C++03 (C++11's
// initializer list feature makes them largely useless).  You can use
// them to create and populate an STL-style container in a single
// expression:
//
//   util::gtl::Container(e1, ..., en)
//
// creates a container of any type with the given n elements, as long
// as the desired type has a constructor that takes a pair of
// iterators (begin, end).
//
//   util::gtl::NewContainer(e1, ..., en)
//
// is similar, except that the container is created on the heap and a
// pointer to it is returned.
//
// These constructs can also be used to create associative containers.
// For example,
//
//   std::unordered_map<string, int> word_lengths =
//       util::gtl::Container(make_pair("hi", 2), make_pair("hello", 5));
//
// results in word_lengths containing two entries:
//
//   word_lengths["hi"] == 2 and word_lengths["hello"] == 5.
//
// The syntax is designed to mimic C++11's initializer list.  For
// example, given a function Foo() that takes a const vector<int>&, in
// C++11 you can write:
//
//   Foo({ 1, 3, 7 });
//
// while this header allows you to write:
//
//   Foo(util::gtl::Container(1, 3, 7));
//
// All values passed to Container() or NewContainer() must have the
// same type.  If the compiler infers different types for them, you'll
// need to disambiguate:
//
//   list<double> foo = util::gtl::Container<double>(1, 3.5, 7);
//
// Currently we support up-to 64 elements in the container.  If this
// is inadequate, you can easily raise the limit by increasing the
// value of max_arity in container_literal_generated.h.pump and
// re-generating container_literal_generated.h.
//
// USE OF THE RETURNED TEMPORARY OBJECT:
//
// The public interface of the object returned by Container() is almost
// exactly the same as that of std::initializer_list.  There is some
// support for generically using a temporary object created by a
// Container() call, even though the type of this object remains
// unspecified and subject to change. The temporary object becomes invalid
// when its full statement has ended. Do not try to extend its
// lifetime by storing it in a variable or capturing a const reference to
// it. While it is valid, it is guaranteed to have the following properties,
// most of which are designed to model those of std::initializer_list.
//
//   Types:
//
//      value_type
//         The cv-qualifier-stripped type of objects passed to the
//         Container() call, or the special tag type
//         util::gtl::ContainerEmptyTag if no arguments were passed.
//      typedef const value_type& const_reference;
//      typedef const value_type& reference;
//
//      const_iterator
//         An STL random access iterator providing const_reference access
//         to the container elements.
//
//      typedef const_iterator iterator;
//
//      typedef size_t size_type;
//
//   MemberFunctions:
//
//      template <typename C> C As() const;
//         Returns a container of type 'C' created from this container object.
//
//   STL container-like member functions are provided:
//
//      size_type size() const;
//      const_iterator begin() const;
//      const_iterator end() const;
//
// Example:
//
//   std::vector<int> vec = ...;
//   util::gtl::c_copy(Container(1, 2, 3, 4, 5),
//                     std::back_inserter(vec));
//   std::map<string, int> m = ...;
//   util::gtl::c_copy(Container(make_pair("a", 1), make_pair("b", 2)),
//                     std::inserter(m, m.end()));
//
// CAVEATS:
//
// 1. When you write
//
//   T array[] = { e1, ..., en };
//
// the elements are guaranteed to be evaluated in the order they
// appear.  This is NOT the case for Container() and NewContainer(),
// as C++ doesn't guarantee the evaluation order of function
// arguments.  Therefore avoid side effects in e1, ..., and en.
//
// 2. Unfortunately, since many STL container classes have multiple
// single-argument constructors, the following syntaxes don't compile
// in C++98 mode:
//
//   vector<int> foo(util::gtl::Container(1, 5, 3));
//   static_cast<vector<int> >(util::gtl::Container(3, 4));
//
// (Note that NewContainer() doesn't suffer from this limitation.)
//
// The latter is especially a problem since it makes it hard to call
// functions overloaded with different container types.  Therefore,
// Container() provides an As<DesiredContainer>() method for
// explicitly specifying the desired container type.  For example:
//
//   // Calls Foo(const vector<int>&).
//   Foo(util::gtl::Container(2, 3).As<vector<int> >());
//   // Calls Foo(const list<int64>&).
//   Foo(util::gtl::Container(4).As<list<int64> >());
//
// 3. For efficiency, Container(...) and NewContainer(...) do NOT make
// a copy of the elements before adding them to the target container -
// it needs the original elements to be around when being converted to
// the target container type.  Therefore make sure the conversion is
// done while the original elements are still alive.  In particular,
// it is unsafe to save the result of Container() to a local variable
// declared with 'auto' if any of the elements are rvalues.  (See
// happens, so it's safer to just avoid 'auto' with Container.
// This also means it is unsafe to use a Container() expression as the
// source of a range-based for loop:
//
//     // BAD CODE: Do not do this.
//     for (const auto& s : util::gtl::Container(string("a"), string("b"))) {
//       ...
//     }
//
// The Container object above will be holding dangling string references.
// The lifetime of the arguments to the Container() call are not
// extended to the scope of the for loop.

#ifndef UTIL_GTL_CONTAINER_LITERAL_H_
#define UTIL_GTL_CONTAINER_LITERAL_H_

#ifdef _GLIBCXX_DEBUG
#include <glibcxx_debug_traits.h>
#endif

#include <stdlib.h>
#include <string.h>
#include <cstddef>
#include <iterator>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/port.h"
#include "supersonic/utils/template_util.h"
#include <type_traits>
#include "supersonic/utils/std_namespace.h"

// Must come after "base/port.h".
#ifdef LANG_CXX11
// Allow uses of C++11 features in this header. DO NOT DO THIS IN YOUR OWN CODE
// for the following reasons:
//  1) C++11 features may have hidden pitfalls, so c-style wants to introduce
//     them gradually, while this pragma allows all of them at once
//  2) This pragma turns off *all* warnings in this file, so you're more likely
//     to write broken code if you use it.
#pragma GCC system_header
#include <initializer_list>  // NOLINT(build/include_order)
#endif

namespace util {
namespace gtl {
namespace internal {

#ifdef _GLIBCXX_DEBUG
using ::glibcxx_debug_traits::IsStrictlyDebugWrapperBase;
#else  // _GLIBCXX_DEBUG
template <typename T> struct IsStrictlyDebugWrapperBase : base::false_ {};
#endif  // _GLIBCXX_DEBUG

// Internal utilities for implementing Container() and NewContainer().
// They are subject to change without notice.  Therefore DO NOT USE
// THEM DIRECTLY.

// A ContainerDerefIterator<T> object can walk through an array of T pointers
// and retrieve the T objects they point to.
template <typename T>
class ContainerDerefIterator {
  // Implementation is mostly boilerplate to satisfy STL
  // random_access_iterator requirements.

  typedef ContainerDerefIterator ME;  // shorten the boilerplate

 public:
  // These typedefs are required by STL algorithms using an iterator.
  typedef std::random_access_iterator_tag iterator_category;
  typedef typename base::remove_cv<T>::type value_type;
  typedef T* pointer;
  typedef T& reference;
  typedef ptrdiff_t difference_type;

  ContainerDerefIterator() : p_(NULL) {}
  explicit ContainerDerefIterator(T* const* ptr) : p_(ptr) {}

  // The compiler-generated copy constructor and assignment operator
  // are exactly what we need, so we don't define our own.

  ME& operator++() { ++p_; return *this; }  // prefix
  ME operator++(int) { ME o(*this); ++p_; return o; }  // postfix
  reference operator*() const { return **p_; }
  pointer operator->() const { return *p_; }
  reference operator[](difference_type n) const { return *p_[n]; }
  ME& operator+=(difference_type n) { p_ += n; return *this; }
  ME& operator-=(difference_type n) { *this += -n; return *this; }

  // required non-members operators
  friend bool operator==(ME a, ME b) { return a.p_ == b.p_; }
  friend bool operator!=(ME a, ME b) { return a.p_ != b.p_; }
  friend bool operator<(ME a, ME b) { return a.p_ < b.p_; }
  friend bool operator>(ME a, ME b) { return a.p_ > b.p_; }
  friend bool operator<=(ME a, ME b) { return a.p_ <= b.p_; }
  friend bool operator>=(ME a, ME b) { return a.p_ >= b.p_; }
  friend ME operator+(ME a, difference_type n) { return a += n; }
  friend ME operator+(difference_type n, ME a) { return a += n; }
  friend ME operator-(ME a, difference_type n) { return a -= n; }
  friend ME operator-(difference_type n, ME a) { return a -= n; }
  friend difference_type operator-(ME a, ME b) { return a.p_ - b.p_; }

 private:
  T* const* p_;
};

// ContainerStorage stores an array of kCount pointers, and creates an
// instance of any desired container type using that container type's
// range constructor.
template <typename T, size_t kCount>
class ContainerStorage {
 public:
  typedef ContainerDerefIterator<const T> const_iterator;
  explicit ContainerStorage(const T* const* p) {
    std::copy(p, p + kCount, ptrs_);
  }

  template <typename DesiredContainer>
  DesiredContainer As() const {
    return DesiredContainer(begin(), end());
  }

  const_iterator begin() const { return const_iterator(ptrs_); }
  const_iterator end() const { return const_iterator(ptrs_ + kCount); }

 private:
  const T* ptrs_[kCount];
};

// ContainerStorage is specialized for the kCount==0 case. This case
// stores no pointers, and creates an instance of the desired container
// type using that container type's default constructor, rather than its
// range constructor.
template <typename T>
class ContainerStorage<T, 0> {
 public:
  typedef ContainerDerefIterator<const T> const_iterator;
  explicit ContainerStorage(const T* const* p) {}

  template <typename DesiredContainer>
  DesiredContainer As() const {
    return DesiredContainer();
  }

  const_iterator begin() const { return const_iterator(); }
  const_iterator end() const { return const_iterator(); }
};

// IsInitializerList<T>::value is true iff T is a specialization of
// std::initializer_list.
template <typename C>
struct IsInitializerList {
  static base::small_ check(...);  // default No
#ifdef LANG_CXX11
  template <typename U> static base::big_ check(std::initializer_list<U>*);
#endif  // LANG_CXX11
  enum { value = sizeof(base::big_) == sizeof(check(static_cast<C*>(0))) };
};

// HasConstIterator<T>::value is true iff T has a typedef const_iterator.
template <typename C>
struct HasConstIterator {
  template <typename U> static base::small_ check(...);  // default No
  template <typename U> static base::big_ check(typename U::const_iterator*);
  enum { value = sizeof(base::big_) == sizeof(check<C>(0)) };
};


// A ContainerIsConvertibleTo<C>::type typedef exists iff the specified
// condition is true for type C.
template <typename C>
struct ContainerIsConvertibleTo
    : base::enable_if<
          !IsStrictlyDebugWrapperBase<C>::value &&
          !IsInitializerList<C>::value &&
          HasConstIterator<C>::value> { };  // NOLINT

// ContainerImpl<T, kCount> implements a collection of kCount values
// whose type is T.  It can be converted to any STL-style container
// that has a constructor taking an iterator range.  It is an approximate
// model of std::initializer_list<T>.
//
// For efficiency, a ContainerImpl<T, kCount> object does not make a
// copy of the values - it saves pointers to them instead.  Therefore
// it must be converted to an STL-style container while the values are
// alive.  We guarantee this by declaring the type ContainerImpl<T,
// kCount> internal and forbidding people to use it directly: since a
// user is not allowed to save an object of this type to a variable
// and use it later, we don't need to worry about the source values
// dying too soon.
template <typename T, size_t kCount>
class ContainerImpl {
 private:
  typedef ContainerStorage<T, kCount> StorageType;

 public:
  typedef typename base::remove_cv<T>::type value_type;
  typedef const T& const_reference;
  typedef const T& reference;
  typedef typename StorageType::const_iterator const_iterator;
  typedef const_iterator iterator;
  typedef size_t size_type;

  explicit ContainerImpl(const T* const* elem_ptrs)
      : storage_(elem_ptrs) {
  }

  // The compiler-generated copy constructor does exactly what we
  // want, so we don't define our own.  The copy constructor is needed
  // as the Container() functions need to return a ContainerImpl
  // object by value.

  // Converts the collection to the desired container type.
  // We aren't able to merge the two #ifdef branches, as default
  // function template argument is a feature new in C++0X.
#ifdef LANG_CXX11
  // We use SFINAE to restrict conversion to container-like types (by
  // testing for the presence of a const_iterator member type) and
  // also to disable conversion to an initializer_list (which also
  // has a const_iterator).  Otherwise code like
  //   vector<int> foo;
  //   foo = Container(1, 2, 3);
  // or
  //   typedef map<int, vector<string> > MapByAge;
  //   MapByAge m;
  //   m.insert(MapByAge::value_type(21, Container("J. Dennett", "A. Liar")));
  // will fail to compile in C++11 due to ambiguous conversion paths
  // (in C++11 vector<T>::operator= is overloaded to take either a
  // vector<T> or an initializer_list<T>, and pair<A, B> has a templated
  // constructor).
  template <typename DesiredContainer,
            typename OnlyIf =
                typename ContainerIsConvertibleTo<DesiredContainer>::type>
#else
  template <typename DesiredContainer>
#endif
  operator DesiredContainer() const {
    return storage_.template As<DesiredContainer>();
  }

  template <typename DesiredContainer>
  DesiredContainer As() const {
    return *this;
  }

  size_type size() const { return kCount; }
  const_iterator begin() const { return storage_.begin(); }
  const_iterator end() const { return storage_.end(); }

 private:
  StorageType storage_;

  ContainerImpl& operator=(const ContainerImpl&);  // disallowed
};

// Like ContainerImpl, except that an object of NewContainerImpl type
// can be converted to an STL-style container on the *heap*.
template <typename T, size_t kCount>
class NewContainerImpl {
 public:
  // elem_ptrs is an array of kCount pointers.
  explicit NewContainerImpl(const T* const* elem_ptrs)
      : container_impl_(elem_ptrs) {}

  // The compiler-generated copy constructor does exactly what we
  // want, so we don't define our own.  The copy constructor is needed
  // as the NewContainer() functions need to return a NewContainerImpl
  // object by value.

  // Converts the collection to the desired container type on the
  // heap; the caller assumes the ownership.
  //
  // We only create the target container when this function is called,
  // so there won't be any leak even if this is never called.  There
  // is no problem if this is called multiple times, as each caller will
  // get its own copy of the container.
  template <typename DesiredContainer>
  operator DesiredContainer*() const {
    return new DesiredContainer(container_impl_);
  }

 private:
  ContainerImpl<T, kCount> container_impl_;

  NewContainerImpl& operator=(const NewContainerImpl&);  // disallowed
};

}  // namespace internal

// Only used as a stand-in for the value_type of a Container that was not
// given any arguments.
// There is no way to create an instance of ContainerEmptyTag.
// A universal conversion operator is provided so that creation of empty
// ranges from the zero-arg form of util::gtl::Container() will compile.
// Example:
//   std::vector<int> v;
//   util::gtl::c_copy(util::gtl::Container(), std::back_inserter(vec));
class ContainerEmptyTag {
 public:
  template <typename T>
  operator T&() const {
    LOG(FATAL) << "Converting a ContainerEmptyTag.";
    // Not reached. In case compiler doesn't know LOG(FATAL) doesn't return.
    return *static_cast<T*>(NULL);
  }
 private:
  ContainerEmptyTag();
};

// The 0-arg Container() and NewContainer() functions are special cases.
inline internal::ContainerImpl<ContainerEmptyTag, 0> Container() {
  return internal::ContainerImpl<ContainerEmptyTag, 0>(NULL);
}
inline internal::NewContainerImpl<ContainerEmptyTag, 0> NewContainer() {
  return internal::NewContainerImpl<ContainerEmptyTag, 0>(NULL);
}

}  // namespace gtl
}  // namespace util

// Defines Container() and NewContainer() for >0 args.
#include "supersonic/utils/container_literal_generated.h"

#endif  // UTIL_GTL_CONTAINER_LITERAL_H_
