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

use std::fs::File;
use std::io::BufWriter;
use std::io::prelude::*;

use LineReader;
use MultiFileReader;
use ByteRangeLineReader;
use multi_file_reader::FindKeyPosition;
use multi_file_reader::get_key;

pub struct OutputFile {
  separator: String,
  verbose: bool,
  output_file: BufWriter<File>,
  output_fields: Vec<(bool, usize)>,
  pub file1: LineReader<ByteRangeLineReader>,
  pub file2: LineReader<ByteRangeLineReader>,
  end_pos: u64,
}

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
      separator: separator.clone(),
      verbose: verbose,
      output_file: BufWriter::new(File::create(output_file_str).unwrap()),
      output_fields: output_fields_str_list.iter().map(|s| _pair_split(s) ).collect(),
      file1: LineReader::new(file1_range, separator.clone(), field1, verbose),
      file2: LineReader::new(
        ByteRangeLineReader::open_range(file2_str_list, start_pos, end_pos, verbose),
        separator.clone(),
        field2,
        verbose
      ),
      end_pos: end_pos
    }
  }

  pub fn get_end_pos(&self) -> u64
  {
    return self.end_pos
  }

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

  pub fn file1_has_current(&self) -> bool
  {
    if self.verbose {
      println!("OutputFile::file1_has_current() = {}", self.file1.has_current());
    }
    self.file1.has_current()
  }

  pub fn file2_has_current(&self) -> bool
  {
    if self.verbose {
      println!("OutputFile::file2_has_current() = {}", self.file2.has_current());
    }
    self.file2.has_current()
  }

  pub fn file1_read_next(&mut self)
  {
    if self.verbose {
      println!("OutputFile::file1_read_next()");
    }
    self.file1.read_next()
  }

  pub fn file2_read_next(&mut self)
  {
    if self.verbose {
      println!("OutputFile::file2_read_next()");
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

#[cfg(test)]
mod test
{
  use std::io::prelude::*;
  use std::fs::File;

  use tempdir::TempDir;

  use ByteRangeLineReader;
  use ReadLiner;


}