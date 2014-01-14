// Copyright 2005 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef UTIL_GTL_FIXEDARRAY_H_
#define UTIL_GTL_FIXEDARRAY_H_

#include <stddef.h>
#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <iterator>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <new>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/manual_constructor.h"

// A FixedArray<T> represents a non-resizable array of T where the
// length of the array does not need to be a compile time constant.
//
// FixedArray allocates small arrays inline, and large arrays on
// the heap.  It is a good replacement for non-standard and deprecated
// uses of alloca() and variable length arrays (a GCC extension).
//
// FixedArray keeps performance fast for small arrays, because it
// avoids heap operations.  It also helps reduce the chances of
// accidentally overflowing your stack if large input is passed to
// your function.
//
// Also, FixedArray is useful for writing portable code.  Not all
// compilers support arrays of dynamic size.

// Most users should not specify an inline_elements argument and let
// FixedArray<> automatically determine the number of elements
// to store inline based on sizeof(T).
//
// If inline_elements is specified, the FixedArray<> implementation
// will store arrays of length <= inline_elements inline.
//
// Finally note that unlike vector<T> FixedArray<T> will not zero-initialize
// simple types like int, double, bool, etc.
//
// Non-POD types will be default-initialized just like regular vectors or
// arrays.
template <typename T, ssize_t inline_elements = -1>
class FixedArray {
 public:
  // For playing nicely with stl:
  typedef T value_type;
  typedef T* iterator;
  typedef T const* const_iterator;
  typedef T& reference;
  typedef T const& const_reference;
  typedef T* pointer;
  typedef T const* const_pointer;
  typedef ptrdiff_t difference_type;
  typedef size_t size_type;

  // Creates an array object that can store "n" elements.
  //
  // FixedArray<T> will not zero-initialiaze POD (simple) types like int,
  // double, bool, etc.
  // Non-POD types will be default-initialized just like regular vectors or
  // arrays.
  explicit FixedArray(size_type n) : rep_(MakeRep(n)) { }

  // Creates an array initialized with the elements from the input
  // range. The size will always be "std::distance(first, last)".
  // REQUIRES: Iter must be a forward_iterator or better.
  template <typename Iter>
  FixedArray(Iter first, Iter last) : rep_(MakeRep(first, last)) { }

  ~FixedArray() {
    CleanUpRep(&rep_);
  }

  // Returns the length of the array.
  size_type size() const { return rep_.size(); }

  // Returns the memory size of the array in bytes.
  size_t memsize() const { return size() * sizeof(value_type); }

  // Returns a pointer to the underlying element array.
  const_pointer get() const { return AsValue(rep_.begin()); }
  pointer get() { return AsValue(rep_.begin()); }

  // REQUIRES: 0 <= i < size()
  // Returns a reference to the "i"th element.
  reference operator[](size_type i) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, size());
    return get()[i];
  }

  // REQUIRES: 0 <= i < size()
  // Returns a reference to the "i"th element.
  const_reference operator[](size_type i) const {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, size());
    return get()[i];
  }

  iterator begin() { return get(); }
  iterator end() { return get() + size(); }

  const_iterator begin() const { return get(); }
  const_iterator end() const { return get() + size(); }

 private:
  // ----------------------------------------
  // HolderTraits:
  // Wrapper to hold elements of type T for the case where T is an array type.
  // If 'T' is an array type, HolderTraits::type is a struct with a 'T v;'.
  // Otherwise, HolderTraits::type is simply 'T'.
  //
  // Maintainer's Note: The simpler solution would be to simply wrap T in a
  // struct whether it's an array or not: 'struct Holder { T v; };', but
  // that causes some paranoid diagnostics to misfire about uses of get(),
  // believing that 'get()' (aka '&rep_.begin().v') is a pointer to a single
  // element, rather than the packed array that it really is.
  // e.g.:
  //
  //     FixedArray<char> buf(1);
  //     sprintf(buf.get(), "foo");
  //
  //     error: call to int __builtin___sprintf_chk(etc...)
  //     will always overflow destination buffer [-Werror]
  //
  class HolderTraits {
    template <typename U>
    struct SelectImpl {
      typedef U type;
      static pointer AsValue(type* p) { return p; }
    };

    // Partial specialization for elements of array type.
    template <typename U, size_t N>
    struct SelectImpl<U[N]> {
      struct Holder { U v[N]; };
      typedef Holder type;
      static pointer AsValue(type* p) { return &p->v; }
    };
    typedef SelectImpl<value_type> Impl;

   public:
    typedef typename Impl::type type;

    static pointer AsValue(type *p) { return Impl::AsValue(p); }

    static_assert(sizeof(type) == sizeof(value_type),
                  "Holder must be same size as value_type");
  };

  typedef typename HolderTraits::type Holder;
  static pointer AsValue(Holder *p) { return HolderTraits::AsValue(p); }

  // ----------------------------------------
  // InlineSpace:
  // Allocate some space, not an array of elements of type T, so that we can
  // skip calling the T constructors and destructors for space we never use.
  // How many elements should we store inline?
  //   a. If not specified, use a default of 256 bytes (256 bytes
  //      seems small enough to not cause stack overflow or unnecessary
  //      stack pollution, while still allowing stack allocation for
  //      reasonably long character arrays.
  //   b. Never use 0 length arrays (not ISO C++)
  //
  class InlineSpace {
    typedef base::ManualConstructor<Holder> Buffer;
    static const size_type kDefaultBytes = 256;

    template <ssize_t N, typename Ignored>
    struct Impl {
      static const size_type kSize = N;
      Buffer* get() { return space_; }
     private:
      static_assert(kSize > 0, "kSize must be positive");
      Buffer space_[kSize];
    };

    // specialize for 0-element case: no 'space_' array.
    template <typename Ignored>
    struct Impl<0, Ignored> {
      static const size_type kSize = 0;
      Buffer* get() { return NULL; }
    };

    // specialize for default (-1) case. Use up to kDefaultBytes.
    template <typename Ignored>
    struct Impl<-1, Ignored> :
        Impl<kDefaultBytes / sizeof(value_type), Ignored> {
    };

    typedef Impl<inline_elements, void> ImplType;

    ImplType space_;

   public:
    static const size_type kSize = ImplType::kSize;

    Holder* get() { return space_.get()[0].get(); }
    void Init(size_type i) { space_.get()[i].Init(); }
    void Destroy(size_type i) { space_.get()[i].Destroy(); }
  };

  static const size_type kInlineElements = InlineSpace::kSize;

  Holder* inline_space() { return inline_space_.get(); }

  // ----------------------------------------
  // Rep:
  // A const Rep object holds FixedArray's size and data pointer.
  //
  class Rep {
   public:
    Rep(size_type n, Holder* p) : n_(n), p_(p) { }
    Holder* begin() const { return p_; }
    Holder* end() const { return p_ + n_; }
    size_type size() const { return n_; }
   private:
    size_type n_;
    Holder* p_;
  };

  void CleanUpRep(const Rep* rep) {
    if (rep->begin() == inline_space()) {
      // Destruction must be in reverse order.
      for (size_type i = rep->size(); i-- > 0; ) {
        inline_space_.Destroy(i);
      }
    } else {
      delete[] rep->begin();
    }
  }

  Rep MakeRep(size_type n) {
    Holder *pa;
    if (n <= kInlineElements) {
      for (size_type i = 0; i < n; ++i) {
        inline_space_.Init(i);
      }
      pa = inline_space();
    } else {
      pa = new Holder[n];
    }
    return Rep(n, pa);
  }

  template <typename Iter>
  Rep MakeRep(Iter first, Iter last, std::forward_iterator_tag) {
    size_type n = std::distance(first, last);
    Holder *pa;
    if (n <= kInlineElements) {
      pa = inline_space();
    } else {
      // Allocate uninitialized space, but don't initialize the elements.
      // We're about to overwrite them with copies of [first, last).
      pa = static_cast<Holder*>(::operator new[] (n * sizeof(Holder)));
    }
    std::uninitialized_copy(first, last, AsValue(pa));
    return Rep(n, pa);
  }

  template <typename Iter>
  Rep MakeRep(Iter first, Iter last) {
    typedef typename std::iterator_traits<Iter> IterTraits;
    return MakeRep(first, last, typename IterTraits::iterator_category());
  }

  // ----------------------------------------
  // Data members
  //
  Rep const rep_;
  InlineSpace inline_space_;

  DISALLOW_COPY_AND_ASSIGN(FixedArray);
};

#endif  // UTIL_GTL_FIXEDARRAY_H_
