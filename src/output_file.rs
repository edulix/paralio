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

use std::fs::File;
use std::io::BufWriter;
use std::io::prelude::*;

use LineReader;
use MultiFileReader;
use ByteRangeLineReader;
use multi_file_reader::FindKeyPosition;
use multi_file_reader::get_key;

/// Struct used to read sequentially from two ByteRangeLineReaders sorted
/// lines, ending in the same key from both readers, writing matches to a third
/// output file.
///
/// Note that a single ByteRangeLineReader can read from multiple files and has
/// itself an end position - that's why it's a range.
///
/// Each line read and written can have multiples fields, separated by the
/// specified separator string.
pub struct OutputFile {
  separator: String,
  verbose: bool,
  output_file: BufWriter<File>,
  output_fields: Vec<(bool, usize)>,
  pub file1: LineReader<ByteRangeLineReader>,
  pub file2: LineReader<ByteRangeLineReader>
}

impl OutputFile {
  /// Initializes an OutputFile object.
  ///
  /// It receives an already split `file1` ByteRangeLineReader, whose start
  /// position is the one at which it currently is when this function is called
  /// and whose end position can be retrieved from the file1
  /// ByteRangeLineReader.
  ///
  /// The file2 is not yet split, but it receives the list of multiple files
  /// that compose it and a start position. With that and then end position of
  /// the file1, the last line in the file1 range is read, the last key of that
  /// line in file1 is obtained, and then the file2 files are scanned using a
  /// binary search to find the line containing corresponding to the file1 last
  /// key, and the position of that line in file2 is used to define the end
  /// of a ByteRangeLineReader for file2.
  ///
  /// If verbose is set to true, some debug output will be shown when operating
  /// with this OutputFile.
  ///
  /// The separator is used to separate the multiple fields when reading from
  /// file1 and file2, and also to separate the output values when writing the
  /// matches in the output file.
  ///
  /// The output file path is specified by the `output_file_str`.
  ///
  /// When a match is added, a line is written in the outputfile, containing
  /// the fields specified in the output_fields_str_list.
  ///
  /// The format of the output_fields_str_list elements is of format
  /// "file_num.field_num", for example "1.0" would specify the first element
  /// of file1, and "2.3" would specify the 4th element of file2.
  pub fn new(
      separator: String,
      verbose: bool,
      output_file_str: String,
      output_fields_str_list: Vec<String>,
      field1: u32,
      file2_str_list: Vec<String>,
      field2: u32,
      file1_range: ByteRangeLineReader,
      start_pos: u64
  ) -> OutputFile
  {
    let separator_char = separator.chars().next().unwrap();

    let last_key: String = get_key(
      &file1_range.last_line(), separator_char, field1 as usize
    ).to_string();
    if verbose {
      println!(
        "OutputFile::new: out={} start_pos={} last_key={}",
        output_file_str,
        start_pos,
        last_key
      );
    }

    let end_pos: u64 = MultiFileReader::find_key_pos(
      last_key, &file2_str_list, separator_char, field2 as usize
    ).unwrap();

    if verbose {
      println!(
        "OutputFile::new: calculated_end_pos={}",
        end_pos,
      );
    }

    return OutputFile
    {
      separator:      separator.clone(),
      verbose:        verbose,
      output_file:    BufWriter::new(File::create(output_file_str).unwrap()),

      output_fields:  output_fields_str_list.iter().map(
        |s| OutputFile::pair_split(s)
      ).collect(),

      file1:          LineReader::new(
        file1_range,
        separator.clone(),
        field1,
        verbose
      ),

      file2: LineReader::new(
        ByteRangeLineReader::open_range(
          file2_str_list,
          start_pos,
          end_pos,
          verbose
        ),
        separator.clone(),
        field2,
        verbose
      )
    }
  }

  /// This is used to process the format in which the output_fields_str_list is
  /// set in the constructor.
  ///
  /// Processes a split, converting it to a pair of (bool, usize) that means
  /// (is_file1, index of the field in split values of the line).
  pub fn pair_split(s: &String) -> (bool, usize)
  {
    let vals: Vec<&str> = (*s).split(".").collect();
    assert!(vals.len() == 2);
    let value: u32 = vals[1].parse().unwrap();
    (vals[0] == "1", value as usize)
  }

  /// Returns the multi-file calculated end position of the file2
  pub fn file2_end(&self) -> u64
  {
    return self.file2.reader().end()
  }

  /// Adds a match for the current lines of file1 and file2, extracting the
  /// required values from both lines according to the configuration given in
  /// the contructor (the input var `output_fields_str_list`) and writing them
  /// into a line in the output file.
  pub fn add_match(&mut self)
  {
    if self.verbose {
      println!("OutputFile::add_match file1_key={}", self.file1_key());
    }
    let line: Vec<String> = self.output_fields.iter()
      .map(
        |&(file_num, field_num)| -> String
        {
          if file_num
          {
            let v = self.file1.field(field_num);
            if self.verbose {
              println!("OutputFile::add_match file_num={} [1], field_num={}, v={}", file_num, field_num, v);
            }
            return v
          }
          else
          {
            let v = self.file2.field(field_num);
            if self.verbose {
              println!("OutputFile::add_match file_num={} [2], field_num={}, v={}", file_num, field_num, v);
            }
            return v
          }
        }
      ).collect();
    self.output_file.write(line.join(self.separator.as_str()).as_bytes()).unwrap();
    self.output_file.write(b"\n").unwrap();
  }

  /// Returns whether there is still a line to be processed in file1 or not
  pub fn file1_has_current(&self) -> bool
  {
    if self.verbose {
      println!("OutputFile::file1_has_current() = {}", self.file1.has_current());
    }
    self.file1.has_current()
  }

  /// Returns whether there is still a line to be processed in file2 or not
  pub fn file2_has_current(&self) -> bool
  {
    if self.verbose {
      println!("OutputFile::file2_has_current() = {}", self.file2.has_current());
    }
    self.file2.has_current()
  }

  /// Tries to read the next line in file1, changing the current line in the
  /// file1 ByteRangeLineReader to the next.
  pub fn file1_read_next(&mut self)
  {
    if self.verbose {
      println!("OutputFile::file1_read_next()");
    }
    self.file1.read_next()
  }

  /// Tries to read the next line in file2, changing the current line in the
  /// file2 ByteRangeLineReader to the next.
  pub fn file2_read_next(&mut self)
  {
    if self.verbose {
      println!("OutputFile::file2_read_next()");
    }
    self.file2.read_next()
  }

  /// Returns a string corresponding with the key field value of the current
  /// file1 line
  pub fn file1_key(&self) -> String
  {
    self.file1.key()
  }


  /// Returns a string corresponding with the key field value of the current
  /// file2 line
  pub fn file2_key(&self) -> String
  {
    self.file2.key()
  }

  /// Returns a string corresponding with the specified field value of the
  /// current file1 line
  pub fn file1_field(&self, i: usize) -> String
  {
    self.file1.field(i)
  }

  /// Returns a string corresponding with the specified field value of the
  /// current file2 line
  pub fn file2_field(&self, i: usize) -> String
  {
    self.file2.field(i)
  }
}

#[cfg(test)]
mod test
{
  use tempdir::TempDir;

  use test_helpers::_assert_file_eq;
  use test_helpers::_write_files;

  use ByteRangeLineReader;
  use OutputFile;

  #[test]
  fn test_files()
  {
    let tmp_dir1 = TempDir::new("output_file").expect("create temp dir 1");
    let tmp_dir2 = TempDir::new("output_file").expect("create temp dir 2");
    let output_file_str = String::from(
      tmp_dir1.path().join("out".to_string()).to_str().unwrap()
    );

    let file_1: &str = "0,4,5";
    let files_1 = _write_files(file_1, &tmp_dir1);
    let file_1_ranges = ByteRangeLineReader::open(
      /*file_list*/ &files_1,
      /*num_readers*/ 1,
      /*verbose*/ true
    );

    let file_2: &str = "1,3,4,";
    let files_2 = _write_files(file_2, &tmp_dir2);

    {
      let mut out = OutputFile::new(
        /*separator*/ String::from(","),
        /*verbose*/ true,
        /*output_file_str*/ output_file_str.clone(),
        /*output_fields_str_list*/ vec![
          String::from("1.0"),
          String::from("2.0"),
          String::from("2.0")
        ],
        /*field1*/ 0,
        /*file2_str_list*/ files_2,
        /*field2*/ 0,
        /*file1_range*/ file_1_ranges[0].clone(),
        /*start_pos*/ 0
      );

      assert_eq!(out.file1_has_current(), true);
      assert_eq!(out.file2_has_current(), true);

      assert_eq!(out.file1_key(), String::from(""));
      assert_eq!(out.file2_key(), String::from(""));

      assert_eq!(out.file1_field(0), String::from(""));
      assert_eq!(out.file2_field(0), String::from(""));
      out.file2_read_next();

      assert_eq!(out.file1_key(), String::from(""));
      assert_eq!(out.file2_key(), String::from("1"));
      assert_eq!(out.file1_field(0), String::from(""));
      assert_eq!(out.file2_field(0), String::from("1"));

      out.add_match(); // adds ",1,1" to output file

      out.file1_read_next();
      out.file2_read_next();

      assert_eq!(out.file1_key(), String::from("0"));
      assert_eq!(out.file2_key(), String::from("3"));
      assert_eq!(out.file1_field(0), String::from("0"));
      assert_eq!(out.file2_field(0), String::from("3"));

      out.add_match(); // adds "0,3,3" to output file
      out.file1_read_next();

      assert_eq!(out.file1_key(), String::from("4"));
      assert_eq!(out.file2_key(), String::from("3"));
      assert_eq!(out.file1_field(0), String::from("4"));
      assert_eq!(out.file2_field(0), String::from("3"));

      out.add_match(); // adds "4,3,3" to output file

      out.file2_read_next();

      assert_eq!(out.file1_key(), String::from("4"));
      assert_eq!(out.file2_key(), String::from("4"));
      assert_eq!(out.file1_field(0), String::from("4"));
      assert_eq!(out.file2_field(0), String::from("4"));

      out.file1_read_next();
      assert_eq!(out.file1_has_current(), true);
      assert_eq!(out.file2_has_current(), true);

      assert_eq!(out.file1_key(), String::from("5"));
      assert_eq!(out.file2_key(), String::from("4"));
      assert_eq!(out.file1_field(0), String::from("5"));
      assert_eq!(out.file2_field(0), String::from("4"));

      out.file1_read_next();
      assert_eq!(out.file1_has_current(), false);
      assert_eq!(out.file2_has_current(), true);

      assert_eq!(out.file1_key(), String::from(""));
      assert_eq!(out.file2_key(), String::from("4"));
      assert_eq!(out.file1_field(0), String::from(""));
      assert_eq!(out.file2_field(0), String::from("4"));

      out.file2_read_next();
      assert_eq!(out.file1_has_current(), false);
      assert_eq!(out.file2_has_current(), false);

      assert_eq!(out.file1_key(), String::from(""));
      assert_eq!(out.file2_key(), String::from(""));
      assert_eq!(out.file1_field(0), String::from(""));
      assert_eq!(out.file2_field(0), String::from(""));
    }

    _assert_file_eq(
      &output_file_str,
      ",1,1\n0,3,3\n4,3,3\n"
    );
  }

  #[test]
  fn test_more_fields_and_chars()
  {
    let tmp_dir1 = TempDir::new("output_file").expect("create temp dir 1");
    let tmp_dir2 = TempDir::new("output_file").expect("create temp dir 2");
    let output_file_str = String::from(
      tmp_dir1.path().join("out".to_string()).to_str().unwrap()
    );

    let file_1: &str = "111;bbbbb;ccc,2222222;5;767u;oo";
    let files_1 = _write_files(file_1, &tmp_dir1);
    let file_1_ranges = ByteRangeLineReader::open(
      /*file_list*/ &files_1,
      /*num_readers*/ 1,
      /*verbose*/ true
    );

    let file_2: &str = "1;aaa;!!!#↓,3;lol;4";
    let files_2 = _write_files(file_2, &tmp_dir2);

    {
      let mut out = OutputFile::new(
        /*separator*/ String::from(";"),
        /*verbose*/ true,
        /*output_file_str*/ output_file_str.clone(),
        /*output_fields_str_list*/ vec![
          String::from("1.2"),
          String::from("1.1"),
          String::from("2.0"),
        ],
        /*field1*/ 1,
        /*file2_str_list*/ files_2,
        /*field2*/ 2,
        /*file1_range*/ file_1_ranges[0].clone(),
        /*start_pos*/ 0
      );

      out.file1_read_next();
      out.file2_read_next();
      assert_eq!(out.file1_has_current(), true);
      assert_eq!(out.file2_has_current(), true);

      assert_eq!(out.file1_key(), String::from("bbbbb"));
      assert_eq!(out.file2_key(), String::from("!!!#↓"));

      assert_eq!(out.file1_field(0), String::from("111"));
      assert_eq!(out.file1_field(1), String::from("bbbbb"));
      assert_eq!(out.file1_field(2), String::from("ccc"));

      assert_eq!(out.file2_field(0), String::from("1"));
      assert_eq!(out.file2_field(1), String::from("aaa"));
      assert_eq!(out.file2_field(2), String::from("!!!#↓"));

      out.file1_read_next();

      assert_eq!(out.file1_key(), String::from("5"));
      assert_eq!(out.file2_key(), String::from("!!!#↓"));

      assert_eq!(out.file1_field(0), String::from("2222222"));
      assert_eq!(out.file1_field(1), String::from("5"));
      assert_eq!(out.file1_field(2), String::from("767u"));
      assert_eq!(out.file1_field(3), String::from("oo"));

      assert_eq!(out.file2_field(0), String::from("1"));
      assert_eq!(out.file2_field(1), String::from("aaa"));
      assert_eq!(out.file2_field(2), String::from("!!!#↓"));

      out.file2_read_next();

      assert_eq!(out.file1_key(), String::from("5"));
      assert_eq!(out.file2_key(), String::from("4"));

      assert_eq!(out.file1_field(0), String::from("2222222"));
      assert_eq!(out.file1_field(1), String::from("5"));
      assert_eq!(out.file1_field(2), String::from("767u"));
      assert_eq!(out.file1_field(3), String::from("oo"));

      assert_eq!(out.file2_field(0), String::from("3"));
      assert_eq!(out.file2_field(1), String::from("lol"));
      assert_eq!(out.file2_field(2), String::from("4"));
    }
  }

}