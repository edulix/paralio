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

use std::io::Result;

use ReadLiner;
use MultiFileReader;

pub struct ByteRangeLineReader
{
  reader: MultiFileReader,
  end: u64,
  current: u64
}

impl ByteRangeLineReader
{
  /**
   * Divides a file in multiple ByteRangeLineReader
   */
  fn open(file_list: Vec<String>, division: u64) -> Vec<ByteRangeLineReader>
  {
    let length = MultiFileReader::len(&file_list);
    // make range a little bigger, so that the last range might be a bit overrun
    // (but of course we will control it) instead of not reading the final bytes
    let range_size: u64 = (length as f64 / division as f64).ceil() as u64;

    return (0..division).map(
      |i|
      {
        let mut ret = ByteRangeLineReader
        {
          reader: MultiFileReader::open(&file_list, i * range_size),
          end: (i + 1) * range_size,
          current: i * range_size
        };
        if i > 0 {
          let mut s: String = String::new();
          ret.read_line(&mut s, false).unwrap();
        }
        return ret
      }
    ).collect()
  }

  fn pos(&self) -> u64
  {
    self.current
  }
}

impl ReadLiner for ByteRangeLineReader
{
  fn read_line(&mut self, buf: &mut String, verbose: bool) -> Result<usize>
  {
    if self.current <= self.end
    {
      let ret = self.reader.read_line(buf, verbose).unwrap();
      self.current += ret as u64;
      return Ok(ret)
    } else
    {
      return Ok(0)
    }
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

  /**
   * Compares the output of a reader with some example string.
   * For example if the string is "0,1,2" it means that the reader will
   * read three lines: "0\n", "1\n" and "2\n" and no more.
   */
  fn assert_eq(reader: &mut ByteRangeLineReader, s: &str)
  {
    println!("assert_eq s={}", s);
    for x in s.to_string().split(",")
    {
      let mut buf = String::new();
      assert_eq!(reader.read_line(&mut buf, false).unwrap(), x.to_string().len() + 1);
      assert_eq!(buf, x.to_string()+"\n");
    }
    let mut buf = String::new();
    assert_eq!(reader.read_line(&mut buf, false).unwrap(), 0);
    assert_eq!(buf, String::new());
  }

  /**
   * Creates a list of files with ints, one per line.
   * Each file is separated by the '|' char, each line by the ',' char
   */
  fn write_files(s: &str, tmp_dir: &TempDir) -> Vec<String>
  {
    println!("write_files s={}", s);
     return s.split('|').enumerate().map(
      |x: (usize, &str)|
      {
        let file_path = String::from(tmp_dir.path().join(x.0.to_string()).to_str().unwrap());
        let mut tmp_file = File::create(file_path.clone()).expect("create temp file");
        println!("write_files x.0={}, x.1={}", x.0, x.1);
        for fline in x.1.split(',')
        {
          tmp_file.write(fline.as_bytes()).unwrap();
          tmp_file.write(b"\n").unwrap();
        }
        return file_path
      }
    ).collect()
  }

  fn test_files(input: &str, output: &str)
  {
    println!("\nwrite_files\n\tinput={}\n\toutput={}", input, output);
    let tmp_dir = TempDir::new("byterange").expect("create temp dir");
    let files = write_files(input, &tmp_dir);
    let output_split: Vec<&str> = output.split('|').collect();

    let mut readers = ByteRangeLineReader::open(files.clone(), output_split.len() as u64);
    assert_eq!(readers.len(), output_split.len());

    for (i, x) in output_split.iter().enumerate()
    {
      assert_eq(&mut readers[i], x);
    }
  }

  /**
   * Testing byte range with one file, one range
   */
  #[test]
  fn test_byte_range()
  {
    let v: Vec<(&str, &str)> = vec![
      (
        "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16",
        "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16"
      ),
      (
        "0,1,2,3,4,5",
        "0,1,2,3|4,5"
      ),
      (
        "0,1,2|3,4,5",
        "0,1,2,3|4,5"
      ),
      (
        "0,1,2,3,4|5",
        "0,1,2,3|4,5"
      ),
      (
        "0,1|2,3,4|5",
        "0,1,2,3|4,5"
      ),
      (
        "0,1|2,3,4|5",
        "0,1,2,3|4,5"
      ),
      (
        "0,1,2|3|4,5,6|7,8,9,10|11,12,13,14,15,16",
        "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16"
      ),
    ];
    for &(a, b) in v.iter()
    {
      test_files(a, b);
    }
  }
}