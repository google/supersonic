// Copyright 2010 Google Inc.  All Rights Reserved
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
// Internal classes to output views to file and read them back using cursor
// interface. Files are output in a binary format optimized for speed of reading
// and writing that should not be used outside of supersonic internals.
//
// Example usage:
//
// Saves data from two views to a file:
// fp = File::OpenOrDie("file_name", "w");
// scoped_ptr<Sink> sink(FileOutput(fp, TAKE_OWNERSHIP));
//
// if (sink->Write(view1).is_failure()) {
//   Handle error...
// }
// if (sink->Write(view2).is_failure()) {
//   Handle error...
// }
// if (sink->Finalize().is_failure()) {
//   Handle error...
// }
//
// Creates a cursor to read the saved data back, deletes file when done:
// fp = File::OpenOrDie("file_name", "r");
// FailureOrOwned<Cursor> result(FileInput(cursor_schema, fp, true,
//                                      HeapBufferAllocator::Get()));

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_FILE_IO_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_FILE_IO_H_

#include "supersonic/utils/basictypes.h"
#include "supersonic/base/exception/result.h"

class File;

namespace supersonic {

class BufferAllocator;
class Cursor;
class TupleSchema;
class Sink;

// The output file should be open for writing by a caller. If Ownership ==
// DO_NOT_TAKE_OWNERSHIP, does not close File* and it is legal to use it after
// FileSink is destroyed (for instance to Seek() to the beginning of the file
// and read data back). In such case caller needs to close the file when it is
// done with it. To save space caller can pass file that compresses its output
// data (for example file/bzip2file/bzip2file::BZip2OutputFile).
// TODO(user): Include information about performance of different compression
// algorithms that can be applied to these files.
Sink* FileOutput(File* output_file, Ownership file_ownership);

// Creates cursor to read from a file that was written with FileSink. Takes
// ownership of the file (caller should not try to close it). If
// delete_when_done is set, file object is deleted from a filesystem when Cursor
// is destroyed.
//
// If a file passed to WriteView was a file that compressed output data, file
// passed to FileInput should be a corresponding file that decompresses input
// data (for example file/bzip2file/bzip2file::BZip2InputFile).
FailureOrOwned<Cursor> FileInput(const TupleSchema& schema,
                                 File* input_file,
                                 const bool delete_when_done,
                                 BufferAllocator* allocator);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_FILE_IO_H_
