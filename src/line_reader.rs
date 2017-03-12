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

use ReadLiner;

/// Helps to iterative line parsing by reading and storing lines for a given
/// ReadLiner object.
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
  /// Creates a LineReader
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

  /// returns if it's possible to continue to read lines or not
  pub fn has_current(&self) -> bool
  {
    !self.finished
  }

  /// reads the next line, storing it internally
  pub fn read_next(&mut self)
  {
    let mut line1 = String::new();
    self.finished = self.reader.read_line(&mut line1, self.verbose).unwrap() == 0;
    line1.pop();
    self.last_parsed_line = line1.split(self.separator.as_str()).map(String::from).collect();
    if self.verbose {
      println!("LineReader::read_next, line_read={}", line1);
    }
  }

  /// returns the index of the key field value
  pub fn key_field(&self) -> usize
  {
    self.key_field
  }

  /// returns the key field value
  pub fn key(&self) -> String
  {
    self.last_parsed_line[self.key_field].clone()
  }

  /// returns the value by index of the last line
  pub fn field(&self, i: usize) -> String
  {
    self.last_parsed_line[i].clone()
  }

  /// Acces to the internal reader as a reference
  pub fn reader(&self) -> &T
  {
    &self.reader
  }
}

#[cfg(test)]
mod test {
  use std;
  use std::slice::Iter;
  use ReadLiner;
  use LineReader;

  impl<'a> ReadLiner for Iter<'a, String>
  {
    fn read_line<'b>(&mut self, buf: &'b mut String, _: bool)
      -> std::io::Result<usize>
    {
      match self.next()
      {
        Some(val) =>
        {
          buf.clone_from(val);
          Ok(buf.len())
        },
        _ => Ok(0)
      }
    }
  }

  #[test]
  fn test_read_lines()
  {
    let values = vec![
      String::from("a,b\n"),
      String::from("c,d\n"),
    ];
    let mut reader = LineReader::new(values.iter(), String::from(","), 0, false);
    assert_eq!(reader.has_current(), true);
    reader.read_next();
    assert_eq!(reader.has_current(), true);

    assert_eq!(reader.key(), String::from("a"));
    assert_eq!(reader.field(0), String::from("a"));
    assert_eq!(reader.has_current(), true);
    assert_eq!(reader.field(0), String::from("a"));
    assert_eq!(reader.field(1), String::from("b"));

    reader.read_next();
    assert_eq!(reader.has_current(), true);
    assert_eq!(reader.key_field(), 0);
    assert_eq!(reader.field(0), String::from("c"));
    assert_eq!(reader.key(), String::from("c"));
    assert_eq!(reader.field(1), String::from("d"));

    reader.read_next();
    reader.reader();
    assert_eq!(reader.has_current(), false);
    assert_eq!(reader.field(0), String::from(""));
    assert_eq!(reader.key(), String::from(""));
  }
}