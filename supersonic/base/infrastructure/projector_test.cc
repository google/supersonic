// Copyright 2010 Google Inc. All Rights Reserved.
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
//

#include "supersonic/base/infrastructure/projector.h"

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/proto/supersonic.pb.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

class ProjectorTest : public testing::Test {
 public:
  void SetUp() {
    schema_0_.add_attribute(Attribute("schema 0 attribute 0", INT64, NULLABLE));
    schema_0_.add_attribute(Attribute("schema 0 attribute 1", STRING,
                                      NULLABLE));

    schema_1_.add_attribute(Attribute("schema 1 attribute 0", INT64, NULLABLE));

    schema_pointers_.push_back(&schema_0_);
    schema_pointers_.push_back(&schema_1_);

    bmsp_.reset(new BoundMultiSourceProjector(schema_pointers_));
    // schema0 is mapped 1-1.
    EXPECT_TRUE(bmsp_->Add(0, 0));
    EXPECT_TRUE(bmsp_->Add(0, 1));
    // schema1's only attribute is mapped twice.
    EXPECT_TRUE(bmsp_->Add(1, 0));
    EXPECT_TRUE(bmsp_->AddAs(1, 0, "schema 1 attribute 0 copy"));
  }

  TupleSchema schema_0_, schema_1_;
  vector<const TupleSchema*> schema_pointers_;
  scoped_ptr<BoundMultiSourceProjector> bmsp_;
  pair<PositionIterator, PositionIterator> positions_;
};

TEST_F(ProjectorTest,
       ReverseMapping) {
  positions_ = bmsp_->ProjectedAttributePositions(0, 0);
  EXPECT_TRUE(util::gtl::Container(0).As<vector<int> >() ==
              vector<int>(positions_.first, positions_.second));

  positions_ = bmsp_->ProjectedAttributePositions(0, 1);
  EXPECT_TRUE(util::gtl::Container(1).As<vector<int> >() ==
              vector<int>(positions_.first, positions_.second));

  positions_ = bmsp_->ProjectedAttributePositions(1, 0);
  EXPECT_TRUE(util::gtl::Container(2, 3).As<vector<int> >() ==
              vector<int>(positions_.first, positions_.second));
}

TEST_F(ProjectorTest,
       AllAttributesProjector) {
  scoped_ptr<const SingleSourceProjector> all_attributes_projector(
      ProjectAllAttributes());
  scoped_ptr<const BoundSingleSourceProjector> bssp(
      SucceedOrDie(all_attributes_projector->Bind(schema_0_)));
  EXPECT_TRUE(schema_0_.EqualByType(bssp->result_schema()));

  TupleSchema schema_0_with_prefix;
  schema_0_with_prefix.add_attribute(
      Attribute("prefix schema 0 attribute 0", INT64, NULLABLE));
  schema_0_with_prefix.add_attribute(
      Attribute("prefix schema 0 attribute 1", STRING, NULLABLE));

  all_attributes_projector.reset(ProjectAllAttributes("prefix "));
  bssp.reset(SucceedOrDie(all_attributes_projector->Bind(schema_0_)));
  EXPECT_TRUE(schema_0_with_prefix.EqualByType(bssp->result_schema()));
}

TEST_F(ProjectorTest,
       AttributesAtPositionsProjector) {
  scoped_ptr<const SingleSourceProjector> position_projector(
      ProjectAttributesAt(util::gtl::Container(1)));
  scoped_ptr<const BoundSingleSourceProjector> bssp(
      SucceedOrDie(position_projector->Bind(schema_0_)));

  TupleSchema result_schema;
  result_schema.add_attribute(
      Attribute("schema 0 attribute 1", STRING, NULLABLE));
  EXPECT_TRUE(result_schema.EqualByType(bssp->result_schema()));
}

TEST_F(ProjectorTest,
       CompoundMultiSourceProjector) {
  CompoundMultiSourceProjector cmsp;
  cmsp.add(0, ProjectAllAttributes());
  cmsp.add(1, ProjectAttributeAt(0));
  cmsp.add(1, ProjectAttributeAtAs(0, "schema 1 attribute 0 copy"));
  scoped_ptr<const BoundMultiSourceProjector> bmsp_copy_(
      SucceedOrDie(cmsp.Bind(schema_pointers_)));
  EXPECT_TRUE(bmsp_->result_schema().EqualByType(bmsp_copy_->result_schema()));
}

TEST_F(ProjectorTest,
       PositionedAttributeProjectorDescription) {
  scoped_ptr<const SingleSourceProjector> projector(
      ProjectAttributeAt(12345678));
  EXPECT_EQ("AttributeAt(12345678)", projector->ToString(true));
}

TEST_F(ProjectorTest, NumberOfProjectionsForAttribute) {
  EXPECT_EQ(1, bmsp_->NumberOfProjectionsForAttribute(0, 0));
  EXPECT_EQ(1, bmsp_->NumberOfProjectionsForAttribute(0, 1));
  EXPECT_EQ(2, bmsp_->NumberOfProjectionsForAttribute(1, 0));
}

}  // namespace supersonic
