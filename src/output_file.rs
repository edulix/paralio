/**
 * Copyright (C) 2017 Eduardo Robles Elvira <edulix@nvotes.com>

 * parallel_pg_select_dump is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License.

 * parallel_pg_select_dump  is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public License
 * along with parallel_pg_select_dump.  If not, see <http://www.gnu.org/licenses/>.
**/
// extern crate itertools;

// use std::fs::File;
// use std::io::BufWriter;
// use std::io::prelude::*;
// use itertools::Itertools;

// use LineReader;
// use MultiFileReader;

pub struct OutputFile {
//   separator: String,
//   verbose: bool,
//   output_file: BufWriter<File>,
//   output_fields: Vec<(bool, usize)>,
//   pub file1: LineReader<MultiFileReader>,
//   pub file2: LineReader<MultiFileReader>,
}
/*
fn _pair_split(s: &String) -> (bool, usize)
{
  let vals: Vec<&str> = (*s).split(".").collect();
  assert!(vals.len() == 2);
  let value: u32 = vals[1].parse().unwrap();
  (vals[0] == "1", value as usize)
}

impl OutputFile {
  pub fn new(
      separator: String,
      verbose: bool,
      output_file_str: String,
      output_fields_str_list: Vec<String>,
      file1_str_list: Vec<String>,
      field1: u32,
      file2_str_list: Vec<String>,
      field2: u32
  ) -> OutputFile {
    OutputFile {
      separator: separator.clone(),
      verbose: verbose,
      output_file: BufWriter::new(File::create(output_file_str).unwrap()),
      output_fields: output_fields_str_list.iter().map(|s| _pair_split(s) ).collect(),
      file1: LineReader::new(MultiFileReader::open(file1_str_list), separator.clone(), field1, verbose),
      file2: LineReader::new(MultiFileReader::open(file2_str_list), separator.clone(), field2, verbose)
    }
  }

  pub fn add_match(&mut self)
  {
    if self.verbose {
      println!("adding a match for key {}", self.file1_key());
    }
    let line: Vec<String> = self.output_fields.iter()
      .map(
        |&(file_num, field_num)| -> String
        {
          if file_num
          {
            self.file1.field(field_num)
          }
          else
          {
            self.file2.field(field_num)
          }
        }
      ).collect();
    self.output_file.write(line.join(self.separator.as_str()).as_bytes()).unwrap();
    self.output_file.write(b"\n").unwrap();
  }

  pub fn file1_has_current(&self) -> bool
  {
    if self.verbose {
      println!("file1_has_current() = {}", self.file1.has_current());
    }
    self.file1.has_current()
  }

  pub fn file2_has_current(&self) -> bool
  {
    if self.verbose {
      println!("file2_has_current() = {}", self.file2.has_current());
    }
    self.file2.has_current()
  }

  pub fn file1_read_next(&mut self)
  {
    if self.verbose {
      println!("file1_read_next()");
    }
    self.file1.read_next()
  }

  pub fn file2_read_next(&mut self)
  {
    if self.verbose {
      println!("file2_read_next()");
    }
    self.file2.read_next()
  }

  pub fn file1_key(&self) -> String
  {
    self.file1.key()
  }

  pub fn file2_key(&self) -> String
  {
    self.file2.key()
  }

  pub fn file1_field(&self, i: usize) -> String
  {
    self.file1.field(i)
  }

  pub fn file2_field(&self, i: usize) -> String
  {
    self.file2.field(i)
  }
}
*/