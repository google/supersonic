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

TEST(TupleSchemaTest, EnumsToDifferentType) {
  EnumDefinition enum_definition;
  enum_definition.AddEntry(0, "a");
  TupleSchema a_schema;
  a_schema.add_attribute(Attribute("attr1", enum_definition, NULLABLE));
  TupleSchema b_schema;
  b_schema.add_attribute(Attribute("attr1", INT32, NULLABLE));
  EXPECT_FALSE(TupleSchema::AreEqual(a_schema, b_schema, false));
}

TEST(TupleSchemaTest, TwoDifferentEnums) {
  EnumDefinition a_enum_definition;
  a_enum_definition.AddEntry(0, "a");
  TupleSchema a_schema;
  a_schema.add_attribute(Attribute("attr1", a_enum_definition, NULLABLE));
  EnumDefinition b_enum_definition;
  b_enum_definition.AddEntry(0, "b");
  TupleSchema b_schema;
  b_schema.add_attribute(Attribute("attr1", b_enum_definition, NULLABLE));
  EXPECT_FALSE(TupleSchema::AreEqual(a_schema, b_schema, false));
}

TEST(TupleSchemaTest, SameEnums) {
  EnumDefinition a_enum_definition;
  a_enum_definition.AddEntry(0, "a");
  TupleSchema a_schema;
  a_schema.add_attribute(Attribute("attr1", a_enum_definition, NULLABLE));
  EnumDefinition b_enum_definition;
  b_enum_definition.AddEntry(0, "a");
  TupleSchema b_schema;
  b_schema.add_attribute(Attribute("attr1", b_enum_definition, NULLABLE));
  EXPECT_TRUE(TupleSchema::AreEqual(a_schema, b_schema, true));
}

}  // namespace supersonic
