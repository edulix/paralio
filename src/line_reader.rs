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

use ReadLiner;

pub struct LineReader<T>
{
  reader: T,
  separator: String,
  key_field: usize,
  last_parsed_line: Vec<String>,
  finished: bool,
  verbose: bool,
}

impl<T: ReadLiner> LineReader<T>
{
  pub fn new(reader: T, separator: String, key_field: u32, verbose: bool) -> LineReader<T>
  {
    LineReader
    {
      reader: reader,
      separator: separator,
      key_field: key_field as usize,
      last_parsed_line: vec![String::new()],
      finished: false,
      verbose: verbose
    }
  }

  pub fn has_current(&self) -> bool
  {
    !self.finished
  }

  pub fn read_next(&mut self)
  {
    let mut line1 = String::new();
    self.finished = self.reader.read_line(&mut line1, self.verbose).unwrap() == 0;
    line1.pop();
    self.last_parsed_line = line1.split(self.separator.as_str()).map(String::from).collect();
    if self.verbose {
      println!("read this line: {}", line1);
    }
  }

  pub fn key(&self) -> String
  {
    self.last_parsed_line[self.key_field].clone()
  }

  pub fn field(&self, i: usize) -> String
  {
    self.last_parsed_line[i].clone()
  }
}
