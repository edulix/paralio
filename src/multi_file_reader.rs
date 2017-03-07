/**
 * Copyright (C) 2017 Eduardo Robles Elvira <edulix@nvotes.com>

 * parallel_pg_select_dump is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.

 * parallel_pg_select_dump  is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public License
 * along with parallel_pg_select_dump.  If not, see
 * <http://www.gnu.org/licenses/>.
**/

use std;
use std::fs;
use std::fs::File;
use std::io::SeekFrom;
use std::io::BufReader;
use std::io::prelude::*;
use std::cmp;

/// Default buffer size to which lines are read from files.
pub static BUFFER_SIZE: usize = 16384;

// A FileInfo is used to indicate the position at which a file with a given
// path starts and ends, with `start` and `end` being multi-file references of
// positions in a MultiFileReader (or a vector of files).
//
// For example, if you have a vector of 2 files:
// - file1, length = 1024 bytes
// - file2, length = 2048 bytes
//
// For that vector of files, the FileInfos could be something like:
// FileInfo { path: "/tmp/file1", start: 0, end: 1024 }
// FileInfo { path: "/tmp/file2", start: 1024, end: 3092 }
//
// FileInfo's are useful for example to be able to seek to a specific
// multi-file position in a MultiFileReader without having to scann through
// the files.
#[derive(Debug, Clone)]
pub struct FileInfo
{
  path: String,
  start: u64,
  end: u64
}

/// Multi file reader allows to read line by line a vector of files just
/// like it was only one file.
///
/// File positions managed in the context of a MultiFileReader are always
/// "multi-file positions", as if all the files were only one, unless specified
/// otherwise.
pub struct MultiFileReader
{
  files_info: Vec<FileInfo>,
  current_file_buffer: BufReader<File>,
  current_file_index: usize,
  current_file_pos: u64
}

/// Trait to read a line to a string, allowing verbose debug output
pub trait ReadLiner
{
  fn read_line(&mut self, buf: &mut String, verbose: bool)
    -> std::io::Result<usize>;
}

impl MultiFileReader
{
  /// Clones a MultiFileReader, replicating the same state as `self`, and thus
  /// reopening the current file and seeking to the current seek position, and
  /// of course also cloning the other fields in the struct.
  pub fn clone(&self) -> MultiFileReader
  {
    // at the end of the file self.current_file_index is too big, so check for
    // that
    let file_index = {
      if self.current_file_index >= self.files_info.len()
      {
        self.files_info.len() - 1
      }
      else
      {
        self.current_file_index
      }
    };
    let mut f: File = File::open(
      self.files_info[file_index].path.clone()
    ).unwrap();
    f.seek(SeekFrom::Start(self.current_file_pos)).unwrap();
    return MultiFileReader
    {
      current_file_buffer: BufReader::new(f),
      files_info: self.files_info.iter().cloned().collect(),
      current_file_index: self.current_file_index,
      current_file_pos: self.current_file_pos
    }
  }

  /// Returns the sum of the lengths of all the files in the reader
  pub fn len(file_list: &Vec<String>) -> u64
  {
    file_list.iter().fold(
      0,
      |accumulator, path| accumulator + fs::metadata(path).unwrap().len()
    )
  }

  /// Returns the vector of file infos for a given vector of file paths.
  pub fn get_files_info(path_list: &Vec<String>) -> Vec<FileInfo>
  {
    // TODO: maybe convert this in a fold
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

  /// Returns the index of the FileInfo from which to read if the caller wants
  /// to read from the multi-file position `pos`.
  ///
  /// If the position is not found then the highest index is returned.
  pub fn find_file_info(files_info: &Vec<FileInfo>, pos: u64) -> usize
  {
    return match files_info.iter().enumerate().find(
      |&(_, file_info)| {file_info.start <= pos && file_info.end > pos}
    ) {
      None => return files_info.len()-1,
      Some((i, _)) => return i
    }
  }

  /// Seeks to multi-file position.
  ///
  /// Seeking might involve closing the currently opened file and opening
  /// another one if the multi-file seek position lies in another file.
  pub fn seek(&mut self, pos: u64)
  {
    // get a valid file_index inside a self.files_info. Usually it's just
    // self.current_file_index, but we use last self.file_info if
    // self.current_file_index is higher or equal to the num of files info.
    //
    // When can that happen? only when we finished to read the files
    let file_index = {
      if self.current_file_index >= self.files_info.len()
      {
        /*return*/ self.files_info.len() - 1
      } else
      {
        /*return*/ self.current_file_index
      }
    };

    // get the start and end multi-file positions of the file_index
    let (start, end) = {
      let ref file_info = self.files_info[file_index];
      (file_info.start, file_info.end)
    };

    // if the current file contains the position to seek, then we just do the
    // seek
    if pos >= start && pos <= end
    {
      self.current_file_pos = pos - start;
      self.current_file_buffer.seek(
        SeekFrom::Start(self.current_file_pos)
      ).unwrap();
    }
    // else, we open the appropiate file and seek it
    else
    {
      self.current_file_index = MultiFileReader::find_file_info(
        &(self.files_info), pos
      );
      let file = {
        let ref file_info = self.files_info[self.current_file_index];
        let mut file = File::open(file_info.path.clone()).unwrap();
        self.current_file_pos = pos - file_info.start;
        file.seek(SeekFrom::Start(self.current_file_pos)).unwrap();
        file
      };
      self.current_file_buffer = BufReader::new(file);
    }
  }

  /// Returns a MultiFileReader for a list of paths. The returned
  /// MultiFileReader will be at the requested multi-file seek position.
  pub fn open(path_list: &Vec<String>, pos: u64) -> MultiFileReader
  {
    let files_info: Vec<FileInfo> = MultiFileReader::get_files_info(path_list);
    let file_index = MultiFileReader::find_file_info(&files_info, pos);
    let current_file_pos: u64;
    let file = {
      let ref file_info = files_info[file_index];
      let mut file = File::open(file_info.path.clone()).unwrap();
      current_file_pos = pos - file_info.start;
      file.seek(SeekFrom::Start(current_file_pos)).unwrap();
      file
    };
    return MultiFileReader
    {
      current_file_buffer: BufReader::new(file),
      files_info: files_info,
      current_file_index: file_index,
      current_file_pos: current_file_pos
    }
  }

  /// Returns the internal mutable reference to the current file buffer
  pub fn get_file_buffer(&mut self) -> &mut BufReader<File>
  {
    return &mut (self.current_file_buffer)
  }

  /// Returns the internal reference to the files info
  pub fn get_own_files_info(&self) -> &Vec<FileInfo>
  {
    return &self.files_info
  }

  /// Returns the size of the MultiFileReader
  pub fn own_len(&self) -> u64
  {
    return self.files_info.last().unwrap().end
  }

  /// Tries to read sequentially to the supplied buffer from the
  /// MultiFileReader starting from the current seek position all the bytes to
  /// fill the buffer.
  ///
  /// This might read from multiple files, but that would be transparent to the
  /// caller, as if reading from only one file.
  pub fn read(&mut self, buf: &mut [u8]) -> std::io::Result<()>
  {
    let mut pos: usize = 0;
    let buf_len = buf.len();
    while pos < buf_len
    {
      let len = self.current_file_buffer.read(&mut buf[pos..buf_len]).unwrap();
      self.current_file_pos += len as u64;
      if len == 0 {
        self.current_file_index += 1;
        if self.current_file_index >= self.files_info.len()
        {
          return Ok(())
        } else {
          let current_file = File::open(
            self.files_info[self.current_file_index].path.clone()
          ).unwrap();
          self.current_file_buffer = BufReader::new(current_file);
          self.current_file_pos = 0;
        }
      }
      pos += len
    }
    return Ok(())
  }
}

impl ReadLiner for MultiFileReader
{
  /// Reads one line to the provided `buf` buffer, returning the size in bytes
  /// of the line read or zero if reached the end of the multi-file reader.
  ///
  /// If the current opened file has no more lines, then it tries to read the
  /// line from the next file recursively.
  fn read_line(&mut self, buf: &mut String, verbose: bool)
    -> std::io::Result<usize>
  {
    match self.current_file_buffer.read_line(buf)
    {
      Ok(bytes) =>
      {
        self.current_file_pos += bytes as u64;
        match bytes
        {
          bytes if bytes > 0 => Ok(bytes),
          bytes =>
          {
            if verbose {
              println!(
                "MultiFileReader::read_line: {:p} read empty line({} bytes): '{}'",
                self, bytes, buf
              );
            }
            self.current_file_index += 1;
            if self.current_file_index >= self.files_info.len()
            {
              Ok(0)
            } else
            {
              if verbose {
                println!("MultiFileReader::read_line: opening file '{}'", self.files_info[self.current_file_index].path.clone());
              }
              let current_file = File::open(
                self.files_info[self.current_file_index].path.clone()
              );
              match current_file
              {
                Ok(file) =>
                {
                  self.current_file_buffer = BufReader::new(file);
                  self.current_file_pos = 0;
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

/// Trait that any struct should implement to be able to find in which
/// multi-file position of a MultiFileReader a specific line is located, given
/// that the multiple files are sorted
///
/// TODO: Make this more general, be able to assume the path_list is part of the
/// underlying struct.
pub trait FindKeyPosition
{
  /// Given a list of path to files that should contain lines with potentially
  /// multiple values per line separated by the given separator and whose lines
  /// are sorted by the value that is always at the given key_field position
  /// of the line, this function returns the position of the line which
  /// contains the given key value.
  fn find_key_pos(
    key: String,
    path_list: &Vec<String>,
    separator: char,
    key_field: usize
  ) -> Option<u64>;
}

/// Returns the last line of the file at the supplied file and the size in
/// bytes of the file.
///
/// Note: It only works if the last line of the file is shorter than
/// `BUFFER_SIZE` in bytes.
pub fn read_file_last_line(path: &String) -> (u64, String)
{
  let file = File::open(path.as_str()).unwrap();
  let mut file_buf = BufReader::new(file);
  let file_size = fs::metadata(path).unwrap().len();

  // ensure because that the buffer we need to seek is not be bigger
  // than the content of the last file.
  let seek_pos: u64 = cmp::max(
    0,
    file_size as i64 - (BUFFER_SIZE as i64)
  ) as u64;

  let mut buf = vec![0; (file_size - seek_pos) as usize];
  file_buf.seek(SeekFrom::Start(seek_pos)).unwrap();
  file_buf.read_exact(&mut buf[0..((file_size - seek_pos) as usize)]).unwrap();

  let lines = String::from_utf8(buf).unwrap();
  let split: Vec<&str> = lines.split('\n').collect();
  let last_line: String = split[split.len()-2].to_string();
  return (file_size, last_line)
}

/// Given a line of text, splits it and gets the value at the given
/// `key_field` index.
pub fn get_key(line: &String, separator: char, key_field: usize) -> &str
{
  let values: Vec<&str> = line.split(separator).collect();
  return values[key_field]
}

impl FindKeyPosition for MultiFileReader
{
  /// Find the seek position of the key in multiple files.
  ///
  /// Asumptions:
  /// - The files are in order.
  /// - The content of the files is one element per line.
  /// - Each element has multiple values, separated by the "separator".
  /// - The key of an element is in the value with the position "key_field".
  /// - The key is a positive integer (0 or more).
  /// - This function should return either the seek position of the key if it is
  ///   found, or the position of the highest value that is lower than the key
  ///   otherwise.
  /// - The last element in the last file must not be bigger than BUFFER_SIZE
  ///   bytes
  /// - files are new-line terminated and contain at least one line
  fn find_key_pos(
      key: String,
      path_list: &Vec<String>,
      separator: char,
      key_field: usize
  ) -> Option<u64>
  {
    // contains:
    // - a key (integer)
    // - the position in bytes of the line containing it (in a MultiFileReader)
    // - the size of the line in bytes
    struct Coordinate {
      key: String,
      pos: u64,
      len: u64
    }
    let mut reader = MultiFileReader::open(path_list, 0);

    // "bottom" and "up" are the limit the search range. we will use a binary
    // search algorithm, and here we set the initial state where bottom is the
    // first line of the first file, and top the last line of the last file
    let mut bottom: Coordinate =
    {

      let mut first_line: String = String::new();
      reader.read_line(&mut first_line, false).unwrap();
      first_line.pop(); // remove \n

      /*return*/ Coordinate
      {
        key: get_key(&first_line, separator, key_field).to_string(),
        pos: 0,
        len: first_line.len() as u64 + 1 /* \n */
      }
    };
    let mut top: Coordinate =
    {
      // getting the last line of the last file is a bit tricky. We basically
      // read the last part of the file into a big enough buffer, so that the
      // buffer contains at least one \n character & split the buffer by that
      // character to get the last line.
      let last_file_path = path_list.last().unwrap().clone();
      let (last_file_size, last_str): (u64, String) = read_file_last_line(&last_file_path);
      let last_key: &str = get_key(&last_str, separator, key_field);

      /*return*/Coordinate
      {
        key: last_key.trim().to_string(),
        pos: (last_file_size as u64) - (last_str.len() as u64) - 1 /* \n */,
        len: last_str.len() as u64 + 1 /* \n */
      }
    };

    // CASE A: if we found the key, return it
    if bottom.key == key
    {
      return Some(bottom.pos)
    }
    // CASE B: if we found the key, return it
    else if top.key == key
    {
      return Some(top.pos)
    }

    loop
    {
      // CASE C: if we didn't found the key but top and bottom are next to each other,
      // return the bottom, because it is the highest number that is equal or
      // lower than the key
      if bottom.pos + bottom.len == top.pos
      {
        return Some(bottom.pos)
      }
      // CASE D: we didn't find the key and still have space to find it, so find an
      // element in the middle of that space and iterate
      else
      {
        // our range includes the bottom line (so we use "bottom.pos" instead of
        // "bottom.pos + bottom.len") because the first line as it might be cut
        // it is going to be discarded anyway, so there's no fear of ending up
        // with the bottom line again
        let middle_pos: u64 = bottom.pos + (top.pos - bottom.pos) / 2;
        reader.seek(middle_pos);

        // discard first line
        let mut cut_pos: u64 = middle_pos;
        {
          let mut discard_line = String::new();
          reader.read_line(&mut discard_line, false).unwrap();
          cut_pos += discard_line.len() as u64;
        }

        // CASE D.1: if we are ending up in the top position, it means that the
        // middle is too close to the top, so we should just use as a cut_pos
        // a line closer to bottom. The easiest way to do it is to use the line
        // next to the bottom line, so that's what we do here.
        //
        // NOTE that at CASE D we know for a fact that there's something in
        // the middle between bottom and pos because otherwise we would be at
        // either case A, B or C but not D.
        if cut_pos == top.pos
        {
          cut_pos = bottom.pos + bottom.len;
          reader.seek(cut_pos);
        }

        let mut cut_line = String::new();
        reader.read_line(&mut cut_line, false).unwrap();
        cut_line.pop(); // remove \n
        let cut_line_key: String = get_key(&cut_line, separator, key_field).to_string();

        // Case D.2
        if cut_line_key == key
        {
          return Some(cut_pos);
        }
        // Case D.3
        else if cut_line_key > key
        {
          top.pos = cut_pos;
          top.key = cut_line_key.clone();
          top.len = cut_line.len() as u64 + 1;
        }
        // Case D.4
        else if cut_line_key < key
        {
          bottom.pos = cut_pos;
          bottom.key = cut_line_key.clone();
          bottom.len = cut_line.len() as u64 + 1;
        }
      }
    }
  }
}

#[cfg(test)]
mod test {
  use std::io::prelude::*;
  use std::fs::File;

  use tempdir::TempDir;

  use MultiFileReader;
  use ReadLiner;
  use multi_file_reader::FindKeyPosition;

  use multi_file_reader::get_key;
  use multi_file_reader::read_file_last_line;


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

    let files_info = MultiFileReader::get_files_info(&files);
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
    let files_info = MultiFileReader::get_files_info(&files);

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
    let mut reader = MultiFileReader::open(&files, 0);

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
    let mut reader = MultiFileReader::open(&files, 8);

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
    let mut reader = MultiFileReader::open(&files, 9);

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
    let mut reader = MultiFileReader::open(&files, 7);

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
    let mut reader = MultiFileReader::open(&files, 0);

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

  #[test]
  fn test_get_key()
  {
    let line = String::from("1,2;3,4");
    let key = get_key(&line, ';', 0);
    assert_eq!(key, "1,2");

    let line = String::from("1,bb;3,4");
    let key = get_key(&line, ',', 1);
    assert_eq!(key, "bb;3");
  }

  #[test]
  fn test_read_file_last_line()
  {
    let data = "aaaaaaaaaaa,b,ccc,dddddddddddddddddddd,erergerg";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);

    let (fsize, last_line) = read_file_last_line(files.first().unwrap());
    assert_eq!(fsize as usize, data.len()+1 /*last new line*/);
    assert_eq!(last_line, "erergerg");
  }

  #[test]
  fn test_find_key()
  {
    let data = "0,1,2,3,4,5,6,7,8,9,10";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);

    let pos = MultiFileReader::find_key_pos("0".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(0));

    let pos = MultiFileReader::find_key_pos("10".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(20));

    let pos = MultiFileReader::find_key_pos("1".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(2));

    let pos = MultiFileReader::find_key_pos("2".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(4));

    let pos = MultiFileReader::find_key_pos("3".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(6));

    let pos = MultiFileReader::find_key_pos("4".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(8));

    let pos = MultiFileReader::find_key_pos("5".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(10));

    let pos = MultiFileReader::find_key_pos("6".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(12));

    let pos = MultiFileReader::find_key_pos("9".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(18));

    let pos = MultiFileReader::find_key_pos("8".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(16));

    let pos = MultiFileReader::find_key_pos("7".to_string(), &files, ',', 0);
    assert_eq!(pos, Some(14));
  }

  #[test]
  fn test_find_key_2chars()
  {
    let data = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);

    let pos = MultiFileReader::find_key_pos("16".to_string(), &files, '|', 0);
    assert_eq!(pos, Some(38));
  }

  #[test]
  fn test_find_key_letters()
  {
    let data = "aaa,aab,aac,abb,ccc,ddde,eeeee,ffff,g";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);

    let pos = MultiFileReader::find_key_pos("ffff".to_string(), &files, '|', 0);
    assert_eq!(pos, Some(31));
  }

  #[test]
  fn test_find_key_non_existant()
  {
    let data = "aaa,aab,aac,abb,ccc,ddde,eeeee,ffff,g";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);

    let pos = MultiFileReader::find_key_pos("fggg".to_string(), &files, '|', 0);
    assert_eq!(pos, Some(31));
  }

  #[test]
  fn test_find_key_non_existant_second_column()
  {
    let data = "whatever#aaa,whatever2#aab,whatever33#aac,whatever55#abb,whatever66#ccc,whatever___#ddde,what__ever#eeeee,a#ffff,whate#g";
    let tmp_dir = TempDir::new("multi_file_reader").expect("create temp dir");
    let files = write_files(data, &tmp_dir);

    let pos = MultiFileReader::find_key_pos("fggg".to_string(), &files, '#', 1);
    assert_eq!(pos, Some(106));
  }
}