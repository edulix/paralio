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
use std::fs::File;
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

impl MultiFileReader
{
  pub fn open(file_list: Vec<String>) -> MultiFileReader
  {
    MultiFileReader
    {
      current_file_buffer: BufReader::new(File::open(file_list[0].clone()).unwrap()),
      file_list: file_list,
      current_file_index: 0
    }
  }

  pub fn read_line(&mut self, buf: &mut String, verbose: bool) -> std::io::Result<usize>
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