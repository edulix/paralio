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
use std::cmp;

use ReadLiner;
use MultiFileReader;
use multi_file_reader::BUFFER_SIZE;

/// ByteRangeLineReader allows to read sequencially only a slice of a
/// MultiFileReader, from the current position to a specified multi-file end
/// position.
pub struct ByteRangeLineReader
{
  reader: MultiFileReader,
  end: u64,
  current: u64
}

impl ByteRangeLineReader
{
  /// Returns a deep clone a ByteRangeLineReader
  pub fn clone(&self) -> ByteRangeLineReader
  {
    return ByteRangeLineReader
    {
      reader: self.reader.clone(),
      end: self.end,
      current: self.current
    }
  }

  /// Divides a file in multiple ByteRangeLineReaders, trying to divide the
  /// readers with roughly the same number of bytes and dividing whole lines.
  pub fn open(file_list: &Vec<String>, num_readers: u64, verbose: bool)
    -> Vec<ByteRangeLineReader>
  {
    let length = MultiFileReader::len(file_list);
    // make range a little bigger, so that the last range might be a bit overrun
    // (but of course we will control it) instead of not reading the final bytes
    let range_size: u64 = (length as f64 / num_readers as f64).ceil() as u64;

    return (0..num_readers).map(
      |i|
      {
        if verbose {
          println!(
            "ByteRangeLineReader::open: it={}, current={} end={}",
            i,
            i * range_size,
            (i+1) * range_size
          );
        }
        let mut ret = ByteRangeLineReader
        {
          reader: MultiFileReader::open(file_list, i * range_size),
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

  /// Creates a ByteRangeLineReader that reads a list of files from some
  /// specific multi-file start & end positions.
  pub fn open_range(
    file_list: Vec<String>,
    start_pos: u64,
    end_pos: u64,
    verbose: bool
  ) -> ByteRangeLineReader
  {
    if verbose {
      println!(
        "ByteRangeLineReader::open_range start_pos={} end_pos={}",
        start_pos,
        end_pos
      );
    }
    ByteRangeLineReader
    {
      reader: MultiFileReader::open(&file_list, start_pos),
      end: end_pos,
      current: start_pos
    }
  }

  /// Returns the multi-file current position
  pub fn pos(&self) -> u64
  {
    self.current
  }

  /// Returns the multi-file end position
  pub fn end(&self) -> u64
  {
    self.end
  }

  /// Read the last line of th range. Note that the problem here is that the end
  /// of the range is "orientative" and not exact. The end is defined by the
  /// next line
  pub fn last_line(&self) -> String
  {
    let seek_pos: u64 = cmp::max(
      0,
      (self.end as i64) - BUFFER_SIZE as i64
    ) as u64;
    let buf_size: usize = (self.end - seek_pos) as usize + cmp::min(
      BUFFER_SIZE as i64,
      cmp::max(
        0,
        self.reader.own_len() as i64 - self.end as i64
      )
     ) as usize;

    let mut buf = vec![0;  buf_size];
    let mut reader = self.reader.clone();
    reader.seek(seek_pos);
    reader.read(&mut buf).unwrap();

    let lines: String;
    let split: Vec<&str>;
    let last_line: String;
    let buf_end_pos: usize = (self.end - seek_pos + 1) as usize;

    // case A: The end of the buffer is the end of the last file, so the last
    // line is the last line in the buffer.
    if buf_end_pos >= buf.len()
    {
      lines = String::from_utf8(buf).unwrap();
      split = lines.split('\n').collect();
      last_line = split[split.len()-2].to_string();
    }
    // case B: The end of the buffer is not the end of the last file, but
    // the end of the byte range is a \n. This means that the next line
    // after that new line char at self.end is what is considered the
    // last line
    else if buf[buf_end_pos] == ('\n' as u8)
    {
      lines = String::from_utf8(buf[buf_end_pos-1..buf.len()].to_vec()).unwrap();
      split = lines.split('\n').collect();
      last_line = split[0].to_string();
    }
    // case C: self.end is not a new line and because all lines end with a new
    // line, it follows that self.end is not the end of the multi-file. This
    // means that the last line starts after the first \n before self.end
    else
    {
      let first_part = String::from_utf8(buf[0..(buf_end_pos - 1)].to_vec()).unwrap();
      let start_pos = first_part.rfind('\n').unwrap();
      lines = String::from_utf8(buf[(start_pos+1)..(buf.len())].to_vec()).unwrap();
      split = lines.split('\n').collect();
      last_line = split[0].to_string();
    }

    return last_line
  }
}

impl ReadLiner for ByteRangeLineReader
{
  /// Reads one line from the ByteRangeLineReader
  fn read_line(&mut self, buf: &mut String, verbose: bool) -> Result<usize>
  {
    if verbose {
      println!("ByteRangeLineReader::read_line {:p}", self);
    }
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

  // Compares the output of a reader with some example string.
  // For example if the string is "0,1,2" it means that the reader will
  // read three lines: "0\n", "1\n" and "2\n" and no more.
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

    let expected_last_line: String = s.to_string().split(",").last().unwrap().to_string();
    assert_eq!(expected_last_line, reader.last_line());
  }

  // Creates a list of files with ints, one per line.
  // Each file is separated by the '|' char, each line by the ',' char
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

    let mut readers = ByteRangeLineReader::open(&files, output_split.len() as u64, true);
    assert_eq!(readers.len(), output_split.len());

    for (i, x) in output_split.iter().enumerate()
    {
      assert_eq(&mut readers[i], x);
    }
  }

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
        "0,1,2,3,4,5,6",
        "0,1,2,3|4,5,6"
      ),
      (
        "0,1,2,3,4,5,6,7",
        "0,1,2,3,4|5,6,7"
      ),
      (
        "0,1,2,3,4,5,6,7,8",
        "0,1,2,3,4|5,6,7,8"
      ),
      (
        "0,1,2,3,4,5,6,7,8,9",
        "0,1,2,3,4,5|6,7,8,9"
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

  #[test]
  fn test_last_line()
  {
    let input: &str = "0,1,2,3,4,5";
    let output: &str = "0,1,2,3,4,5";
    let tmp_dir = TempDir::new("byterange").expect("create temp dir");
    let files = write_files(input, &tmp_dir);
    let output_split: Vec<&str> = output.split('|').collect();

    let mut readers = ByteRangeLineReader::open(&files, output_split.len() as u64, true);
    assert_eq!(readers.len(), output_split.len());

    let mut buf = String::new();
    readers[0].read_line(&mut buf, false).unwrap();
    assert_eq!(buf, String::from("0\n"));

    let mut buf = String::new();
    readers[0].read_line(&mut buf, false).unwrap();
    assert_eq!(buf, String::from("1\n"));

    assert_eq!(readers[0].last_line(), String::from("5"));

    let mut buf = String::new();
    readers[0].read_line(&mut buf, false).unwrap();
    assert_eq!(buf, String::from("2\n"));
  }

}
