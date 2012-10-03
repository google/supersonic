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
//
// File format: Schema is not written to a file, user must pass correct schema
// to the FileInput() function. Each view is split into chunks, each chunk
// contains at most kMaxChunkRowCount rows. Chunks are written one after another
// to a file, so FileInputCursor can read them separately and needs to allocate
// a block of only kMaxChunkRowCount rows. For each chunk, a file contains a
// number of rows in it, followed by data for each column. If column is
// nullable, its nullability table is written first. For not variable length
// columns, data is simply written as a single array. For variable length
// columns, first array with lengths of each element is written to the file (0
// for null and empty strings), then single array with data for all not null and
// not empty elements.
//
// Note - this is suitable only for temporary storage; for long-term storage
// other formats should be used.

#include "supersonic/cursor/infrastructure/file_io.h"
#include "supersonic/cursor/infrastructure/file_io-internal.h"

#include <stddef.h>

#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
#include <limits>
using std::numeric_limits;

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/base/memory/arena.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/cursor/infrastructure/writer.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/file.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

// File output.
// --------------------------------------------------------------------

static const int kMaxChunkRowCount = 8192;

namespace {

static Exception* NewFileOutputException() {
  return (new Exception(ERROR_GENERAL_IO_ERROR,
                        "Writing view to the output file failed."));
}

// Writes data to a file, returns true on success.
// TODO(user): this thing will require a bit of specialization for BIT.
FailureOrVoid Write(const void* buffer, const size_t length,
                    File* output_file) {
  if (!(output_file->Write(buffer, length) == length)) {
    THROW(NewFileOutputException());
  }
  return Success();
}

FailureOrVoid WriteUint64(const uint64 datum, File* output_file) {
  PROPAGATE_ON_FAILURE(Write(&datum, sizeof(uint64), output_file));
  return Success();
}

FailureOrVoid WriteRowCount(const rowcount_t datum, File* output_file) {
  PROPAGATE_ON_FAILURE(WriteUint64(static_cast<uint64>(datum), output_file));
  return Success();
}

// Writes bit-data to a file, returns true on success.
// Two versions for the two possible representations of the is_null data.
FailureOrVoid WriteBits(bit_pointer::bit_const_ptr buffer,
                        const size_t bits,
                        File* output_file) {
  // We cast to a single byte, shift is always between 0 and 31.
  DCHECK(0 <= buffer.shift() && buffer.shift() <= 31);
  char shift = buffer.shift();
  PROPAGATE_ON_FAILURE(Write(&shift, sizeof(shift), output_file));
  PROPAGATE_ON_FAILURE(Write(buffer.data(),
                             bit_pointer::byte_count(bits + shift),
                             output_file));
  return Success();
}

FailureOrVoid WriteBits(const bool* buffer,
                        const size_t bits,
                        File* output_file) {
  PROPAGATE_ON_FAILURE(Write(buffer, bits, output_file));
  return Success();
}

FailureOrVoid WriteVariableLengthData(const Column& column,
                                      const rowcount_t row_count,
                                      File* output_file) {
  const StringPiece* string_pieces = column.variable_length_data();

  // Write lengths of strings first (0 for null and empty strings).
  for (rowid_t row = 0; row < row_count; row++) {
    uint64 string_length = 0;
    if (!(column.is_null() != NULL && column.is_null()[row])) {
      string_length = string_pieces[row].length();
    }
    PROPAGATE_ON_FAILURE(WriteUint64(string_length, output_file));
  }

  // Write data for all not null, not empty strings in a single continuous
  // block.
  for (rowid_t row = 0; row < row_count; row++) {
    if ((column.is_null() != NULL && column.is_null()[row])
        || string_pieces[row].length() == 0) {
      continue;
    }
    PROPAGATE_ON_FAILURE(
        Write(string_pieces[row].data(), string_pieces[row].length(),
               output_file));
  }
  return Success();
}

FailureOrVoid WriteColumn(const Column& column,
                          const rowcount_t row_count,
                          File* output_file) {
  const TypeInfo& type_info = column.type_info();
  if (column.attribute().is_nullable()) {
    CHECK(column.is_null() != NULL) <<
        "Column that according to schema is nullable does not have "
        "a nullability table. TODO(dawidk): handle this situation better; "
        "the contract allows that to happen (and be semantically equivalent "
        "to a nullability table that is all false); we should be able to "
        "serialize and deserialize it correctly";
    PROPAGATE_ON_FAILURE(WriteBits(column.is_null(), row_count, output_file));
  }

  if (type_info.is_variable_length()) {
    PROPAGATE_ON_FAILURE(WriteVariableLengthData(column, row_count,
                                                 output_file));
  } else {
    PROPAGATE_ON_FAILURE(
        Write(column.data().raw(), type_info.size() * row_count, output_file));
  }
  return Success();
}

}  // namespace

// Writes view to a file, splits it into chunks not bigger then
// max_chunk_row_count. This function exist only to test splitting logic.
FailureOrVoid WriteViewWithMaxChunkRowCount(
    const View& view,
    const rowcount_t max_chunk_row_count,
    File* output_file) {
  ViewIterator iterator(view);
  while (iterator.next(max_chunk_row_count)) {
    PROPAGATE_ON_FAILURE(WriteRowCount(iterator.row_count(), output_file));
    for (int i = 0; i < iterator.column_count(); ++i) {
      PROPAGATE_ON_FAILURE(WriteColumn(iterator.column(i),
                                       iterator.row_count(),
                                       output_file));
    }
  }
  return Success();
}

class FileSink : public Sink {
 public:
  FileSink(File* output_file, Ownership file_ownership)
      : output_file_(output_file),
        file_ownership_(file_ownership) {
    CHECK_NOTNULL(output_file_);
  }

  ~FileSink() {
    CHECK(output_file_ == NULL);
  }

  // Writes view to the output file, splits it into small chunks that can be
  // read back without allocating much memory.
  virtual FailureOr<rowcount_t> Write(const View& data) {
    PROPAGATE_ON_FAILURE(
        WriteViewWithMaxChunkRowCount(data, kMaxChunkRowCount, output_file_));
    return Success(data.row_count());
  }

  virtual FailureOrVoid Finalize() {
    if (file_ownership_ == TAKE_OWNERSHIP) {
      if (!output_file_->Close()) {
        output_file_ = NULL;
        THROW(new Exception(ERROR_GENERAL_IO_ERROR, "Error closing the file."));
      }
    }
    output_file_ = NULL;
    return Success();
  }

 private:
  File* output_file_;
  Ownership file_ownership_;

  DISALLOW_COPY_AND_ASSIGN(FileSink);
};

Sink* FileOutput(File* output_file, Ownership file_ownership) {
  return new FileSink(output_file, file_ownership);
}

// File input.
// --------------------------------------------------------------------

namespace {

Exception* NewFileInputException() {
  return new Exception(ERROR_GENERAL_IO_ERROR,
                       "Reading cursor's data from the input file failed.");
}

enum ReadResult { DATA, END_OF_FILE };

// Reads data from a file.
FailureOr<ReadResult> Read(File* input_file,
                           const size_t length,
                           void* buffer) {
  int64 read = input_file->Read(buffer, length);
  if (read == length) return Success(DATA);
  if (read == 0 && input_file->eof()) return Success(END_OF_FILE);
  THROW(NewFileInputException());
}

FailureOr<ReadResult> ReadUint64(File* input_file, uint64* datum) {
  FailureOr<ReadResult> result = Read(input_file, sizeof(uint64), datum);
  PROPAGATE_ON_FAILURE(result);
  return result;
}

FailureOr<ReadResult> ReadRowCount(File* input_file, rowcount_t* datum) {
  uint64 datum_uint64;
  FailureOr<ReadResult> result = ReadUint64(input_file, &datum_uint64);
  PROPAGATE_ON_FAILURE(result);
  if (result.get() == END_OF_FILE) return Success(END_OF_FILE);
  if (datum_uint64 > std::numeric_limits<rowcount_t>::max()) {
    THROW(new Exception(ERROR_GENERAL_IO_ERROR, "Row ID overflow."));
  }
  *datum = static_cast<rowcount_t>(datum_uint64);
  return Success(DATA);
}

// Reads bit-data from a file, returns true on success.
// Two versions for two possible representations of the is_null column.
FailureOr<ReadResult> ReadBits(File* input_file,
                               const size_t bits,
                               bit_pointer::bit_ptr buffer) {
  // We cast to a single byte, shift is always between 0 and 31.
  char shift;
  FailureOr<ReadResult> result = Read(input_file, sizeof(shift), &shift);
  PROPAGATE_ON_FAILURE(result);
  if (result.get() == END_OF_FILE) return Success(END_OF_FILE);
  bit_pointer::bit_array temp_buffer;
  // This is equivalent to malloc-ing the memory. The cost of the malloc should
  // be smaller than the cost of the disk read anyway.
  // We have to use a temporary buffer to deal with the possibility that the
  // input bit data and output bit data have different bit-shifts.
  if (!temp_buffer.Reallocate(bits + shift, HeapBufferAllocator::Get())) {
    THROW(new Exception(ERROR_MEMORY_EXCEEDED, "Can't allocate bit buffer"));
  }
  result = Read(input_file,
                bit_pointer::byte_count(bits + shift),
                temp_buffer.mutable_data().data());
  PROPAGATE_ON_FAILURE(result);
  if (result.get() == END_OF_FILE) return Success(END_OF_FILE);
  bit_pointer::FillFrom(buffer, temp_buffer.mutable_data() + shift, bits);
  return Success(DATA);
}

FailureOr<ReadResult> ReadBits(File* input_file,
                               const size_t bits,
                               bool* buffer) {
  return Read(input_file, bits, buffer);
}

FailureOrVoid ExpectData(FailureOr<ReadResult> result) {
  PROPAGATE_ON_FAILURE(result);
  if (result.get() == END_OF_FILE) {
    THROW(new Exception(ERROR_GENERAL_IO_ERROR, "Premature END_OF_FILE."));
  }
  return Success();
}

}  // namespace

class FileInputCursor : public BasicCursor {
 public:
  FileInputCursor(Block* block, File* input_file, bool delete_when_done)
      : BasicCursor(block->schema()),
        block_(block),
        input_file_(input_file),
        delete_when_done_(delete_when_done),
        rows_pending_in_block_(0),
        first_pending_row_offset_(0),
        strings_length_buffer_(new uint64[kMaxChunkRowCount]) {}

  virtual ~FileInputCursor() {
    if (delete_when_done_) {
      if (!input_file_->Delete()) {
        LOG(WARNING) << "Failed to delete file in FileInputCursor.";
      }
    }
    input_file_->Close();
  }

  virtual ResultView Next(const rowcount_t max_row_count);

  virtual CursorId GetCursorId() const { return FILE_INPUT; }

 private:
  FailureOrVoid ReadColumn(OwnedColumn* column, const rowcount_t row_count);

  FailureOrVoid ReadVariableLengthData(OwnedColumn* column,
                                       const rowcount_t row_count);

  scoped_ptr<Block> block_;
  File* input_file_;
  bool delete_when_done_;
  rowcount_t rows_pending_in_block_;
  rowcount_t first_pending_row_offset_;
  // Temporary buffer used by ReadVariableLengthColumn() to store strings'
  // lengths.
  scoped_array<uint64> strings_length_buffer_;

  DISALLOW_COPY_AND_ASSIGN(FileInputCursor);
};

FailureOrOwned<Cursor> FileInput(const TupleSchema& schema,
                                 File* input_file,
                                 const bool delete_when_done,
                                 BufferAllocator* allocator) {
  CHECK_NOTNULL(input_file);
  CHECK_NOTNULL(allocator);
  scoped_ptr<Block> block(new Block(schema, allocator));
  if (!block.get() || !block->Reallocate(kMaxChunkRowCount)) {
    input_file->Close();
    THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                        "Block allocation for FileInputCursor failed."));
  }
  return Success(
      new FileInputCursor(block.release(), input_file, delete_when_done));
}

// Reads chunk of data from the input file. If chunk contains more rows then
// max_row_count, excessive rows are returned in subsequent calls to Next().
ResultView FileInputCursor::Next(const rowcount_t max_row_count) {
  PROPAGATE_ON_FAILURE(ThrowIfInterrupted());
  if (rows_pending_in_block_ != 0) {
    rowcount_t rows_to_return = min(max_row_count, rows_pending_in_block_);
    my_view()->ResetFromSubRange(block_->view(),
                                 first_pending_row_offset_,
                                 rows_to_return);
    first_pending_row_offset_ += rows_to_return;
    rows_pending_in_block_ -= rows_to_return;
    return ResultView::Success(my_view());
  }

  rowcount_t chunk_row_count = 0;
  FailureOr<ReadResult> result = ReadRowCount(input_file_, &chunk_row_count);
  PROPAGATE_ON_FAILURE(result);
  if (result.get() == END_OF_FILE) return ResultView::EOS();

  // These errors should never happen if the input file was written with the
  // FileSink.
  if (chunk_row_count == 0) {
    THROW(new Exception(
        ERROR_GENERAL_IO_ERROR,
        "Reading cursor's data from the input file failed. "
        "Chunk of size 0."));
  }
  if (chunk_row_count > block_->row_capacity()) {
    THROW(new Exception(
        ERROR_GENERAL_IO_ERROR,
        "Reading cursor's data from the input file failed. "
        "Input chunk too large."));
  }

  block_->ResetArenas();
  for (int i = 0; i < schema().attribute_count(); ++i) {
    PROPAGATE_ON_FAILURE(ReadColumn(block_->mutable_column(i),
                                    chunk_row_count));
  }

  rowcount_t rows_to_return = min(chunk_row_count, max_row_count);
  my_view()->ResetFromSubRange(block_->view(), 0, rows_to_return);
  rows_pending_in_block_ = chunk_row_count - rows_to_return;
  first_pending_row_offset_ = rows_to_return;
  return ResultView::Success(my_view());
}

FailureOrVoid FileInputCursor::ReadColumn(OwnedColumn* column,
                                          const rowcount_t row_count) {
  if (column->content().attribute().is_nullable()) {
    PROPAGATE_ON_FAILURE(ExpectData(
        ReadBits(input_file_, row_count, column->mutable_is_null())));
  }

  const TypeInfo& type_info = column->content().type_info();
  if (type_info.is_variable_length()) {
    PROPAGATE_ON_FAILURE(ReadVariableLengthData(column, row_count));
  } else {
    PROPAGATE_ON_FAILURE(ExpectData(
        Read(input_file_, row_count << type_info.log2_size(),
             column->mutable_data())));
  }
  return Success();
}

FailureOrVoid FileInputCursor::ReadVariableLengthData(
    OwnedColumn* column,
    const rowcount_t row_count) {
  PROPAGATE_ON_FAILURE(ExpectData(
      Read(input_file_, sizeof(uint64) * row_count,
           strings_length_buffer_.get())));

  size_t total_strings_length = 0;
  for (size_t row = 0; row < row_count; ++row) {
    total_strings_length += strings_length_buffer_[row];
  }
  char* strings_data = NULL;

  if (total_strings_length != 0) {
    strings_data = static_cast<char*>(
        column->arena()->AllocateBytes(total_strings_length));
    if (!strings_data) {
      THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                          "Arena allocation for FileInputCursor failed."));
    }
    PROPAGATE_ON_FAILURE(ExpectData(
        Read(input_file_, total_strings_length, strings_data)));
  }

  StringPiece* string_pieces = column->mutable_variable_length_data();
  size_t offset = 0;
  for (size_t row = 0; row < row_count; ++row) {
    string_pieces[row] = StringPiece(strings_data + offset,
                                     strings_length_buffer_[row]);
    offset += strings_length_buffer_[row];
  }
  return Success();
}

}  // namespace supersonic
