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

/*
 * Multi file reader allows to read line by line a vector of files just
 * like it was only one file
 */
pub struct MultiFileReader {
  file_list: Vec<String>,
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

  pub fn open(file_list: Vec<String>, start_offset: u64) -> MultiFileReader
  {
    let mut size: u64 = 0;
    file_list.clone().iter().enumerate().fold(
      (0, None),
      |
        acc_tuple: (/*size*/u64, /*reader*/Option<MultiFileReader>),
        path_tuple: (/*index*/usize, /*path*/&String)
      | {
        match acc_tuple.1 {
          None => {
            let f_size = fs::metadata(path_tuple.1).unwrap().len();
            if f_size + acc_tuple.0 > start_offset
            {
              let mut f = File::open(path_tuple.1).unwrap();
              f.seek(SeekFrom::Start(start_offset - acc_tuple.0));
              return (
                0,
                Some(
                  MultiFileReader
                  {
                    current_file_buffer: BufReader::new(f),
                    file_list: file_list.clone(),
                    current_file_index: path_tuple.0
                  }
                )
              )
            } else
            {
              return ((acc_tuple.0 + f_size) as u64, None)
            }
          },
          Some(_) => return acc_tuple
        }
      }
    ).1.unwrap()
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
            if self.current_file_index >= self.file_list.len()
            {
              Ok(0)
            } else
            {
              if verbose {
                println!("opening file '{}'", self.file_list[self.current_file_index].clone());
              }
              let current_file = File::open(self.file_list[self.current_file_index].clone());
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

#[cfg(test)]
mod test {
  use ReadLiner;
  use std;

  #[test]
  fn it_works() {
    struct LineVecStr {
      data: Vec<String>,
      index: usize
    }

    impl LineVecStr {
      fn new(data: Vec<String>) -> LineVecStr {
        LineVecStr {
          data: data.clone(),
          index: 0
        }
      }
    }

    impl ReadLiner for LineVecStr {
      fn read_line(&mut self, buf: &mut String, verbose: bool) -> std::io::Result<usize>
      {
        if self.index < self.data.len() {
          buf.push_str(self.data[self.index].as_str());
          self.index += 1;
          Ok(buf.len())
        } else {
          Ok(0)
        }
      }
    }

    let mut t: LineVecStr = LineVecStr::new(vec![String::from("12"), String::from("3")]);
    let mut s = String::new();
    assert!(t.read_line(&mut s, false).unwrap() == 2);
    assert!(s.as_str() == "12");

    let tt: &mut ReadLiner = &mut t as &mut ReadLiner;
    let mut ss = String::new();
    assert!((*tt).read_line(&mut ss, false).unwrap() == 1);
    assert!(ss.as_str() == "3");
  }
}