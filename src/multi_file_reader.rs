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

use std;
use std::fs;
use std::fs::File;
use std::io::SeekFrom;
use std::io::BufReader;
use std::io::prelude::*;
use std::cmp;

static BUFFER_SIZE: usize = 16384;

pub struct FileInfo {
  path: String,
  start: u64,
  end: u64
}

/*
 * Multi file reader allows to read line by line a vector of files just
 * like it was only one file
 */
pub struct MultiFileReader {
  files_info: Vec<FileInfo>,
  current_file_buffer: BufReader<File>,
  current_file_index: usize
}

pub trait ReadLiner {
  fn read_line(&mut self, buf: &mut String, verbose: bool) -> std::io::Result<usize>;
}

impl MultiFileReader
{
  pub fn len(file_list: Vec<String>) -> u64
  {
    file_list.iter().fold(
      0,
      |accumulator, path| accumulator + fs::metadata(path).unwrap().len()
    )
  }

  pub fn get_files_info(path_list: Vec<String>) -> Vec<FileInfo>
  {
    let mut ret: Vec<FileInfo> = Vec::with_capacity(path_list.len());
    let mut last_end: u64 = 0;
    for path in path_list.iter()
    {
      let fsize = fs::metadata(path).unwrap().len();
      ret.push(
        FileInfo
        {
          path: path.clone(),
          start: last_end,
          end: last_end + fsize
        }
      );
      last_end += fsize;
    }
    return ret
  }

  pub fn find_file_info(files_info: &Vec<FileInfo>, pos: u64) -> usize
  {
    return match files_info.iter().enumerate().find(
      |&(_, file_info)| {file_info.start <= pos && file_info.end > pos}
    ) {
      None => return files_info.len()-1,
      Some((i, _)) => return i
    }
  }

  pub fn seek(&mut self, pos: u64)
  {
    let (start, end) = {
      let ref file_info = self.files_info[self.current_file_index];
      (file_info.start, file_info.end)
    };
    if pos >= start && pos <= end
    {
      self.current_file_buffer.seek(SeekFrom::Start(pos - start)).unwrap();
    }
    else
    {
      self.current_file_index = MultiFileReader::find_file_info(&(self.files_info), pos);
      let file = {
        let ref file_info = self.files_info[self.current_file_index];
        let mut file = File::open(file_info.path.clone()).unwrap();
        file.seek(SeekFrom::Start(pos - file_info.start)).unwrap();
        file
      };
      self.current_file_buffer = BufReader::new(file);
    }
  }

  pub fn open(path_list: Vec<String>, pos: u64) -> MultiFileReader
  {
    let files_info: Vec<FileInfo> = MultiFileReader::get_files_info(path_list);
    let file_index = MultiFileReader::find_file_info(&files_info, pos);
    let file = {
      let ref file_info = files_info[file_index];
      let mut file = File::open(file_info.path.clone()).unwrap();
      file.seek(SeekFrom::Start(pos - file_info.start)).unwrap();
      file
    };
    return MultiFileReader
    {
      current_file_buffer: BufReader::new(file),
      files_info: files_info,
      current_file_index: file_index
    }
  }
}

impl ReadLiner for MultiFileReader
{
  fn read_line(&mut self, buf: &mut String, verbose: bool) -> std::io::Result<usize>
  {
    match self.current_file_buffer.read_line(buf)
    {
      Ok(bytes) =>
      {
        match bytes
        {
          bytes if bytes > 0 => Ok(bytes),
          _ =>
          {
            self.current_file_index += 1;
            if self.current_file_index >= self.files_info.len()
            {
              Ok(0)
            } else
            {
              if verbose {
                println!("opening file '{}'", self.files_info[self.current_file_index].path.clone());
              }
              let current_file = File::open(self.files_info[self.current_file_index].path.clone());
              match current_file
              {
                Ok(file) =>
                {
                  self.current_file_buffer = BufReader::new(file);
                  self.read_line(buf, verbose)
                },
                Err(why) => Err(why),
              }
            }
          }
        }
      },
      Err(error) => Err(error)
    }
  }
}

// trait FindKeyPosition
// {
//   fn find_key_pos(key: u64, path_list: Vec<String>, separator: String, key_field: usize) -> u64;
// }


// impl FindKeyPosition for MultiFileReader
// {
  /**
   * Find the seek position of the key in multiple files.
   *
   * Asumptions:
   * - The files are in order.
   * - The content of the files is one element per line.
   * - Each element has multiple values, separated by the "separator".
   * - The key of an element is in the value with the position "key_field".
   * - The key is numeric.
   * - This function should return either the seek position of the key if it is
   *   found, or the position of the highest value that is lower than the key
   *   otherwise.
   * - The last element in the last file must not be bigger than 16384 bytes
   */
//   fn find_key_pos(key: u64, path_list: Vec<String>, separator: String, key_field: usize) -> Option<u64>
//   {
//     let bottom: u64 = 0;
//     let top: u64 = {
//       let mut buf = vec![0; BUFFER_SIZE];
//       let last_file = File::open(path_list.last().clone());
//
//       let last_line = cmp::max(0, fs::metadata(path).unwrap().len() - buf.len());
//     };
//     loop {
//
//     }
//
//     return None
//   }
// }

#[cfg(test)]
mod test {
  use std::io::prelude::*;
  use std::fs::File;
  use MultiFileReader;
  use ReadLiner;

  use tempdir::TempDir;

  /**
   * Creates a list of files with ints, one per line.
   * Each file is separated by the '|' char, each line by the ',' char
   */
  pub fn write_files(s: &str, tmp_dir: &TempDir) -> Vec<String>
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

  #[test]
  fn test_multifile_get_files_info()
  {
    let data = "0,1,2|3|4,5,6|7,8,9,10|11,12,13,14,15,16";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);

    let files_info = MultiFileReader::get_files_info(files);
    assert_eq!(files_info[0].start, 0);
    assert_eq!(files_info[0].end, 6);
    assert_eq!(files_info[1].start, 6);
    assert_eq!(files_info[1].end, 8);
    assert_eq!(files_info[2].start, 8);
  }

  #[test]
  fn test_multifile_find_file_info()
  {
    let data = "0,1,2|3|4,5,6|7,8,9,10|11,12,13,14,15,16";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);
    let files_info = MultiFileReader::get_files_info(files);

    assert_eq!(MultiFileReader::find_file_info(&files_info, 0), 0);
    assert_eq!(MultiFileReader::find_file_info(&files_info, 1), 0);
    assert_eq!(MultiFileReader::find_file_info(&files_info, 5), 0);
    assert_eq!(MultiFileReader::find_file_info(&files_info, 6), 1);
    assert_eq!(MultiFileReader::find_file_info(&files_info, 7), 1);
    assert_eq!(MultiFileReader::find_file_info(&files_info, 8), 2);
  }

  #[test]
  fn test_multifile_read_line()
  {
    let data = "0,1,2|3|4,5,6|7";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);
    let mut reader = MultiFileReader::open(files, 0);

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "0\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "1\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "2\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "3\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "4\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "5\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "6\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "7\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "");
  }

  #[test]
  fn test_multifile_read_line_openseek()
  {
    let data = "0,1,2|3|4,5,6|7";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);
    let mut reader = MultiFileReader::open(files, 8);

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "4\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "5\n");
  }

  #[test]
  fn test_multifile_read_line_openseek2()
  {
    let data = "0,1,2|3|4,5,6|7";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);
    let mut reader = MultiFileReader::open(files, 9);

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "5\n");
  }

  #[test]
  fn test_multifile_read_line_openseek3()
  {
    let data = "0,1,2|3|4,5,6|7";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);
    let mut reader = MultiFileReader::open(files, 7);

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "4\n");
  }

  #[test]
  fn test_multifile_seek()
  {
    let data = "0,1,2|3|4,5,6|7,8,9,10|11,12,13,14,15,16";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);
    let mut reader = MultiFileReader::open(files, 0);

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "0\n");

    reader.seek(8);

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "4\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "5\n");

    reader.seek(9);

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "5\n");

    reader.seek(7);

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "\n");

    let mut s = String::new();
    reader.read_line(&mut s, false).expect("reading a line");
    assert_eq!(s.as_str(), "4\n");
  }
}