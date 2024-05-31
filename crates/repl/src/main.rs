use std::{io, ptr};
use std::borrow::ToOwned;
use std::clone::Clone;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::os::unix::fs::OpenOptionsExt;
use std::rc::Rc;

use scan_fmt::scan_fmt;

use crate::Error::{ExecuteError, PrepareError, PrepareStringTooLong, TableFull};
use crate::ExecuteResult::{ExecuteSuccess, ExecuteTableFull};

const ID_SIZE: usize = size_of::<i32>();
const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

enum MetaCommandResult {
    MetaCommandSuccess,
    MetaCommandUnrecognizedCommand,
    MetaNoCommand,
}

#[derive(Debug)]
enum StatementType {
    StatementInsert,
    StatementSelect,
}

enum PrepareResult {
    PrepareSuccess,
    PrepareUnrecognizedStatement,
    PrepareSyntaxError,
    PrepareStringTooLong,
    PrepareNegativeId,
}

#[derive(Debug)]
enum ExecuteResult {
    ExecuteSuccess,
    ExecuteTableFull,
    ExecuteFail,
}

#[derive(Debug)]
enum Error {
    MetaCommandError,
    MetaCommandExit,
    MetaNoCommand,
    PrepareError,
    ExecuteError,
    PrepareStringTooLong,
    PrepareNegativeId,
    TableFull,
    DbOpenError,
}

enum RowSlotError {}

#[derive(Debug)]
struct Row {
    id: i32,
    username: String,
    email: String,
}

impl Row {
    fn new() -> Self {
        Row {
            id: 0,
            username: String::with_capacity(32),
            email: String::with_capacity(255),
        }
    }
}

#[derive(Debug)]
struct Statement {
    statement_type: Option<StatementType>,
    row_to_insert: Row,
}

impl Statement {
    fn new() -> Statement {
        Statement {
            statement_type: None,
            row_to_insert: Row {
                id: 0,
                username: String::with_capacity(32),
                email: String::with_capacity(255),
            },
        }
    }
}


#[derive(Debug)]
struct InputBuffer {
    buffer: Option<String>,
    buffer_length: i32,
    input_length: i32,
}

impl InputBuffer {
    fn new() -> InputBuffer {
        InputBuffer {
            buffer: None,
            buffer_length: 0,
            input_length: 0,
        }
    }
}

#[derive(Debug)]
struct Pager {
    file: Rc<File>,
    file_length: u64,
    pages: Vec<Option<Box<[u8; PAGE_SIZE]>>>,
}

#[derive(Debug)]
struct Table {
    num_rows: usize,
    pager: Pager,
}

impl Pager {
    fn new(file: Rc<File>, file_length: u64) -> Self {
        Pager {
            file,
            file_length,
            pages: vec![None; TABLE_MAX_PAGES],
        }
    }
    fn pager_flush(&mut self, page_num: usize, page_size: usize) -> io::Result<()> {
        if (page_num > TABLE_MAX_PAGES) {
            eprintln!("Tried to flush a out of bound page");
            std::process::exit(1);
        }
        if self.pages[page_num].is_none() {
            eprintln!("Tried to flush null page");
            std::process::exit(1);
        }
        let offset = (page_num * PAGE_SIZE) as u64;
        let page = self.pages[page_num].as_ref().unwrap();
        let mut file = Rc::get_mut(&mut self.file).unwrap();
        file.seek(SeekFrom::Start(offset))?;
        println!("{:?}", &page[page_num]);
        let bytes_written = file.write(&page[..page_size])?;
        if bytes_written != page_size {
            eprintln!("Error writing: only {} bytes written out of {}", bytes_written, page_size);
            std::process::exit(1);
        }
        Ok(())
    }
}

fn get_page(pager: &mut Pager, page_num: usize) -> Result<&mut [u8; PAGE_SIZE], io::Error> {
    if *&pager.pages[page_num].is_none() {
        let mut page: Box<[u8; PAGE_SIZE]> = Box::new([0; PAGE_SIZE]);
        let mut num_pages = pager.file_length as usize / PAGE_SIZE;
        if pager.file_length as usize % PAGE_SIZE != 0 {
            num_pages += 1;
        }
        if page_num < num_pages {
            let offset = (page_num * PAGE_SIZE) as u64;
            let mut file = Rc::get_mut(&mut pager.file).unwrap();
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut *page).unwrap()
        }
        pager.pages[page_num] = Some(page);
    }
    Ok(pager.pages[page_num].as_mut().unwrap())
}

fn pager_open(filename: &str) -> io::Result<Pager> {
    let mut file = Rc::new(OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .open(filename)?
    );
    let file_length = Rc::get_mut(&mut file).unwrap().seek(SeekFrom::End(0))?;
    Ok(Pager::new(file, file_length))
}

impl Table {
    // fn new() -> Self {
    //     Table {
    //         num_rows: 0,
    //         pager: Pager::new(),
    //     }
    // }
    fn open_from_file(file_name: &str) -> Result<Self, Error> {
        let pager = pager_open(file_name);
        match pager {
            Ok(pager) => {
                Ok(Table {
                    num_rows: pager.file_length as usize / PAGE_SIZE * ROWS_PER_PAGE,
                    pager,
                })
            }
            Err(_) => {
                Err(Error::DbOpenError)
            }
        }

    }
    fn row_slot(&mut self, row_id: usize) -> Result<&mut [u8], ExecuteResult> {
        let page_num = row_id / ROWS_PER_PAGE;
        if page_num > TABLE_MAX_PAGES {
            return Err(ExecuteTableFull);
        }
        let page = get_page(&mut self.pager, page_num);
        match page {
            Ok(page) => {
                let row_offset = row_id % ROWS_PER_PAGE;
                let byte_offset = row_offset * ROW_SIZE;
                Ok(&mut page[byte_offset..byte_offset + ROW_SIZE])
            }
            Err(_err) => {
                Err(ExecuteResult::ExecuteFail)
            }
        }
    }
}

fn dp_open(filename: &str) -> Result<Table, Error> {
    Table::open_from_file(filename)
}

fn db_close(table: &mut Table) {
    let mut pager = &mut table.pager;
    let num_full_pages = table.num_rows / ROWS_PER_PAGE;
    for i in 0..num_full_pages {
        if pager.pages[i].is_none() {
            continue;
        }
        pager.pager_flush(i, PAGE_SIZE).expect("Flush Error");
        pager.pages[i] = None;
    }
    let additional_rows = table.num_rows % ROWS_PER_PAGE;
    if additional_rows > 0 {
        let page_num = num_full_pages;
        if !pager.pages[page_num].is_none() {
            pager.pager_flush(page_num, PAGE_SIZE).expect("Flush Error");
            pager.pages[page_num] = None;
        }
    }
}

fn main() {
    let mut db_name = String::new();
    io::stdin().read_line(&mut db_name).unwrap();
    let mut table = dp_open(&db_name.trim_end());
    match table {
        Ok(mut table) => {
            loop {
                let mut input_buffer = InputBuffer::new();
                read_input(&mut input_buffer);
                let res = process_input(&mut input_buffer, &mut table);
                match res {
                    Ok(_) => {}
                    Err(Error::MetaCommandError) => {
                        break;
                    }
                    Err(Error::MetaNoCommand) => {
                        break;
                    }
                    Err(Error::MetaCommandExit) => {
                        break;
                    }
                    _ => {}
                }
            }
            db_close(&mut table);
        }
        Err(err) => {
            println!("{:?}", err);
        }
    }
}

fn process_input(input_buffer: &mut InputBuffer, table: &mut Table) -> Result<(), Error> {
    match do_meta_command(&input_buffer) {
        MetaCommandResult::MetaCommandSuccess => {
            Err(Error::MetaCommandExit)
        }
        MetaCommandResult::MetaCommandUnrecognizedCommand => {
            Ok(Error::MetaCommandError)
        }
        MetaCommandResult::MetaNoCommand => {
            println!("No command is selected");
            Err(Error::MetaNoCommand)
        }
    }?;
    let mut statement = Statement::new();
    match prepare_statement(&input_buffer, &mut statement) {
        PrepareResult::PrepareSuccess => {
            // println!("Prepare success {:?}", statement);
            Ok(())
        }
        PrepareResult::PrepareUnrecognizedStatement => {
            println!("Unrecognized keyword at start of {:?}", &input_buffer.buffer.clone());
            Ok(())
        }
        PrepareResult::PrepareSyntaxError => {
            println!("Syntax error: could not parse statement");
            Err(PrepareError)
        }
        PrepareResult::PrepareStringTooLong => {
            Err(PrepareStringTooLong)
        }
        PrepareResult::PrepareNegativeId => {
            Err(Error::PrepareNegativeId)
        }
    }?;
    match execute_statement(&mut statement, table) {
        ExecuteSuccess => {
            // println!("Query executed successfully");
            Ok(())
        }
        ExecuteResult::ExecuteTableFull => {
            println!("Insert is not allowed, Table is full");
            Err(TableFull)
        }
        ExecuteResult::ExecuteFail => {
            println!("Query execution failed");
            Err(ExecuteError)
        }
    }?;
    Ok(())
}

fn print_prompt() {
    print!("db -> ");
    io::stdout().flush().unwrap();
}

fn read_input(buffer: &mut InputBuffer) {
    let mut input = String::new();
    print_prompt();
    let n = io::stdin().read_line(&mut input).unwrap();
    if n == 1 {
        buffer.buffer = None;
    } else {
        buffer.input_length = n as i32 - 1;
        buffer.buffer = Some(input.trim_end().to_owned());
    }
}

fn do_meta_command(input_buffer: &InputBuffer) -> MetaCommandResult {
    if let Some(buffer_data) = &input_buffer.buffer {
        if buffer_data.eq(".exit") {
            MetaCommandResult::MetaCommandSuccess
        } else {
            MetaCommandResult::MetaCommandUnrecognizedCommand
        }
    } else {
        MetaCommandResult::MetaNoCommand
    }
}

fn prepare_statement(input_buffer: &InputBuffer, statement: &mut Statement) -> PrepareResult {
    if let Some(buffer_data) = &input_buffer.buffer {
        return match &buffer_data[..6] {
            "insert" => {
                statement.statement_type = Some(StatementType::StatementInsert);
                match scan_fmt!(buffer_data, "insert {} {} {}", i32, String, String) {
                    Ok((id, name, email)) => {
                        if id < 0 {
                            return PrepareResult::PrepareNegativeId;
                        }
                        if email.len() > EMAIL_SIZE || name.len() > USERNAME_SIZE {
                            return PrepareResult::PrepareStringTooLong;
                        }
                        statement.row_to_insert.id = id;
                        statement.row_to_insert.email = email;
                        statement.row_to_insert.username = name;
                        PrepareResult::PrepareSuccess
                    }
                    Err(_) => {
                        PrepareResult::PrepareSyntaxError
                    }
                }
            }
            "select" => {
                statement.statement_type = Some(StatementType::StatementSelect);
                PrepareResult::PrepareSuccess
            }
            _ => PrepareResult::PrepareUnrecognizedStatement
        };
    }
    PrepareResult::PrepareUnrecognizedStatement
}

fn execute_statement(statement: &Statement, table: &mut Table) -> ExecuteResult {
    return match &statement.statement_type {
        None => {
            println!("The statement is not valid for execution");
            ExecuteResult::ExecuteFail
        }
        Some(stmt) => {
            match stmt {
                StatementType::StatementInsert => {
                    execute_insert(statement, table)
                }
                StatementType::StatementSelect => {
                    execute_select(statement, table)
                }
            }
        }
    };
}

fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult {
    if table.num_rows >= TABLE_MAX_ROWS {
        return ExecuteTableFull;
    }
    serialize_row(&statement.row_to_insert, table.row_slot(table.num_rows).unwrap());
    table.num_rows += 1;

    ExecuteSuccess
}

fn execute_select(statement: &Statement, table: &mut Table) -> ExecuteResult {
    let mut row = Row::new();
    for i in 0..table.num_rows {
        deserialize_row(table.row_slot(i).unwrap(), &mut row);
        println!("Row {} {:?}", i, row);
    }
    ExecuteSuccess
}

fn serialize_row(source: &Row, destination: &mut [u8]) {
    unsafe {
        ptr::copy_nonoverlapping(
            &source.id as *const i32 as *const u8,
            destination.as_mut_ptr().add(ID_OFFSET),
            ID_SIZE,
        );
        let username_bytes = source.username.as_bytes();
        ptr::copy_nonoverlapping(
            username_bytes.as_ptr(),
            destination.as_mut_ptr().add(USERNAME_OFFSET),
            USERNAME_SIZE,
        );
        let email_bytes = source.email.as_bytes();
        let email_length = email_bytes.len().min(EMAIL_SIZE);
        ptr::copy_nonoverlapping(
            email_bytes.as_ptr(),
            destination.as_mut_ptr().add(EMAIL_OFFSET),
            email_length,
        );
        if email_length < EMAIL_SIZE {
            ptr::write_bytes(destination.as_mut_ptr().add(EMAIL_OFFSET + email_length), 0, EMAIL_SIZE - email_length);
        }
    }
}

fn deserialize_row(source: &[u8], destination: &mut Row) {
    unsafe {
        ptr::copy_nonoverlapping(
            source.as_ptr().add(ID_OFFSET),
            &mut destination.id as *mut i32 as *mut u8,
            ID_SIZE,
        );

        let username_bytes = &source[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE];
        destination.username = String::from_utf8_lossy(username_bytes).trim_end_matches('\0').to_string();

        let email_bytes = &source[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE];
        destination.email = String::from_utf8_lossy(email_bytes).trim_end_matches('\0').to_string();
    }
}

#[cfg(test)]
mod tests {
    use crate::{Error, InputBuffer, process_input, Table};

    #[test]
    fn test_inserting_and_retrieving_a_row() {
        let mut table = Table::new();
        let mut input_buffer = InputBuffer::new();
        let str = String::from("insert 1 bala bala@gmail.com");
        input_buffer.buffer_length = str.len() as i32;
        input_buffer.buffer = Some(str);
        let _ = process_input(&mut input_buffer, &mut table);
        assert_eq!(table.num_rows, 1);
    }

    #[test]
    fn test_table_full() {
        let mut table = Table::new();
        let mut input_buffer = InputBuffer::new();
        for i in 0..1400 {
            let str = format!("insert {} bala bala@gmail.com", i);
            input_buffer.buffer_length = str.len() as i32;
            input_buffer.buffer = Some(str);
            let _ = process_input(&mut input_buffer, &mut table);
        }
        let res = process_input(&mut input_buffer, &mut table);
        assert!(matches!(res, Err(Error::TableFull)));
    }

    #[test]
    fn allows_inserting_strings_with_maximum_length() {
        let long_username = "a".repeat(33);
        let long_email = "a".repeat(255);
        let mut table = Table::new();
        let mut input_buffer = InputBuffer::new();
        let str = format!("insert 1 {} {}", long_username, long_email);
        input_buffer.buffer_length = str.len() as i32;
        input_buffer.buffer = Some(str);
        let res = process_input(&mut input_buffer, &mut table);
        assert!(matches!(res, Err(Error::PrepareStringTooLong)));
    }

    #[test]
    fn allows_inserting_negative_id() {
        let long_username = "a".to_string();
        let long_email = "b".to_string();
        let mut table = Table::new();
        let mut input_buffer = InputBuffer::new();
        let str = format!("insert -10 {} {}", long_username, long_email);
        input_buffer.buffer_length = str.len() as i32;
        input_buffer.buffer = Some(str);
        let res = process_input(&mut input_buffer, &mut table);
        assert!(matches!(res, Err(Error::PrepareNegativeId)));
    }
}