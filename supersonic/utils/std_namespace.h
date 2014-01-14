#ifndef SUPERSONIC_OPENSOURCE_AUXILIARY_STD_NAMESPACE_H_
#define SUPERSONIC_OPENSOURCE_AUXILIARY_STD_NAMESPACE_H_

// This file is only for compatibility between internal and open-sourced
// build environment. Stuff should be incrementally removed from this file.

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <deque>
using std::deque;
#include <limits>
#include "supersonic/utils/std_namespace.h"
#include <list>
#include "supersonic/utils/std_namespace.h"
#include <map>
using std::map;
#include <memory>
#include <type_traits>
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <utility>
#include "supersonic/utils/std_namespace.h"
#include <queue>
#include "supersonic/utils/std_namespace.h"
#include <vector>
using std::vector;

namespace supersonic {

// From <algorithm>.
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;

// From <deque>.
using std::deque;

// From <list>.
using std::list;

// From <map>.
using std::map;
using std::multimap;


// From <set>.
using std::multiset;;
using std::set;

// From <limits>.
using std::numeric_limits;

// From <queue>.
using std::priority_queue;

// From <utility>.
using std::make_pair;
using std::pair;

// From <memory>.
using std::shared_ptr; //  NOLINT

}  // namespace supersonic

namespace base {

using std::remove_cv;
using std::is_pod;
using std::is_reference;
using std::remove_reference;
using std::enable_if;
using std::is_integral;
using std::remove_pointer;
using std::is_enum;
using std::is_same;

// Right now these macros are no-ops, and mostly just document the fact
// these types are PODs, for human use.  They may be made more contentful
// later.  The typedef is just to make it legal to put a semicolon after
// these macros.
#define DECLARE_POD(TypeName) typedef int Dummy_Type_For_DECLARE_POD
#define DECLARE_NESTED_POD(TypeName) DECLARE_POD(TypeName)
#define PROPAGATE_POD_FROM_TEMPLATE_ARGUMENT(TemplateName)             \
    typedef int Dummy_Type_For_PROPAGATE_POD_FROM_TEMPLATE_ARGUMENT
#define ENFORCE_POD(TypeName) typedef int Dummy_Type_For_ENFORCE_POD

}  // namespace base

#endif  // SUPERSONIC_OPENSOURCE_AUXILIARY_STD_NAMESPACE_H_
