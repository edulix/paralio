//! This is the documentation for the `paralio` crate.
//!
//! The paralio crate implements a series of command line tool that take
//! advantage of multiple cores using multi-threading parallelization
//! techniques.

/**
 * Copyright (C) 2017 Eduardo Robles Elvira <edulix@nvotes.com>

 * paralio is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License.

 * paralio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public License
 * along with paralio.  If not, see <http://www.gnu.org/licenses/>.
**/

extern crate tempdir;

mod multi_file_reader;
mod line_reader;
mod output_file;
mod byte_range_line_reader;
mod parallel_join;

pub use multi_file_reader::MultiFileReader;
pub use multi_file_reader::ReadLiner;
pub use line_reader::LineReader;
pub use byte_range_line_reader::ByteRangeLineReader;
pub use output_file::OutputFile;
pub use parallel_join::execute_parallel_join;