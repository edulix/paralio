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

use std::io::prelude::*;
use std::fs::File;
use std::path::Path;

use tempdir::TempDir;

// compares a file's contents with a string
pub fn _assert_file_eq(path: &String, content: &str)
{
  let mut out_f = File::open(path.as_str()).unwrap();
  let mut contents: Vec<u8> = Vec::new();
  out_f.read_to_end(&mut contents).unwrap();
  let filestr = String::from_utf8(contents).unwrap();
  assert_eq!(filestr, content);
}

// Creates a list of files with ints, one per line.
// Each file is separated by the '|' char, each line by the ',' char
pub fn _write_files(s: &str, tmp_dir: &TempDir) -> Vec<String>
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

// compares multiple consecutive file's contents with a string. Each file is
// a number (0, 1, 2, etc) and their content is separated in `content` by a
// '|' character.
pub fn _assert_files_eq(dir_path: &String, content: &str)
{
  for (i, file_content) in content.split('|').enumerate()
  {
    let file_string = Path::new(&dir_path).join(i.to_string());
    let file_path = file_string.to_str().unwrap();
    let mut out_f = File::open(file_path).unwrap();
    let mut contents: Vec<u8> = Vec::new();
    out_f.read_to_end(&mut contents).unwrap();
    let filestr = String::from_utf8(contents).unwrap();
    assert_eq!(filestr, file_content);
  }
}
