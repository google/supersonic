#include "supersonic/base/infrastructure/tuple_schema.h"

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/strcat.h"
#include "gtest/gtest.h"
#include "supersonic/utils/pointer_vector.h"

namespace supersonic {

static const int32 kNumAttributes = 100000;

TEST(TupleSchemaTest, BigSchema) {
  TupleSchema schema;
  for (int i = 0; i < kNumAttributes; ++i) {
    schema.add_attribute(Attribute(StrCat(i), INT32, NULLABLE));
  }
  util::gtl::PointerVector<const BoundSingleSourceProjector> projectors;
  for (int i = 0; i < schema.attribute_count(); ++i) {
    scoped_ptr<const SingleSourceProjector> projector(CHECK_NOTNULL(
        ProjectAttributeAt(i)));
    projectors.push_back(common::SucceedOrDie(
        projector->Bind(schema)));
  }
}

}  // namespace supersonic
