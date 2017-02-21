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
#[macro_use]
extern crate clap;

extern crate paralio;

// use clap::App;

// use paralio::OutputFile;

fn main()
{
/*  let yaml = load_yaml!("cli.yml");
  let matches = App::from_yaml(yaml).get_matches();

  let file1_str_list: Vec<String> = matches.values_of("file1").unwrap()
    .map(String::from).collect();

  let file2_str_list: Vec<String> = matches.values_of("file2").unwrap()
    .map(String::from).collect();

  let separator = matches.value_of("separator").unwrap().to_string();

  let field1: u32 = matches.value_of("field1").unwrap().parse().unwrap();

  let field2: u32 = matches.value_of("field2").unwrap().parse().unwrap();

  let output_fields_str_list: Vec<String> = matches.values_of("output-fields")
    .unwrap().map(String::from).collect();

  let output_path = matches.value_of("output").unwrap().to_string();

  let verbose: bool = matches.is_present("verbose");

  let mut out = OutputFile::new(
    separator,
    verbose,
    output_path,
    output_fields_str_list,
    file1_str_list,
    field1,
    file2_str_list,
    field2
  );
  out.file1_read_next();
  out.file2_read_next();

  while out.file1_has_current() && out.file2_has_current()
  {
    let key1 = out.file1_key();
    let key2 = out.file2_key();
    if verbose {
      println!("key1: {} key2: {}", key1, key2);
    }

    match (Some(key1), Some(key2))
    {
      (Some(ref key1), Some(ref key2)) if key1 < key2 => {
        out.file1_read_next();
      },
      (Some(ref key1), Some(ref key2)) if key1 == key2 => {
        out.add_match();
        out.file1_read_next();
        out.file2_read_next();
      },
      (Some(ref key1), Some(ref key2)) if key1 > key2 => {
        out.file2_read_next();
      },
      _=> break,
    }
  }*/

  // TODO
  // let njobs: i32 = matches.value_of("jobs").unwrap().parse().unwrap();

}