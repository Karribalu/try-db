use std::{io, ptr};
use std::borrow::ToOwned;
use std::clone::Clone;
use std::mem::size_of;

use scan_fmt::scan_fmt;
use crate::ExecuteResult::ExecuteSuccess;

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

enum MetaCommandResult{
    MetaCommandSuccess,
    MetaCommandUnrecognizedCommand,
    MetaNoCommand
}
#[derive(Debug)]
enum StatementType{
    StatementInsert,
    StatementSelect
}
enum PrepareResult{
    PrepareSuccess,
    PrepareUnrecognizedStatement,
    PrepareSyntaxError
}

enum ExecuteResult{
    ExecuteSuccess,
    ExecuteTableFull,
    ExecuteFail
}
#[derive(Debug)]
struct Row{
    id: i32,
    username: String,
    email: String
}
impl Row{
    fn new() -> Self{
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
    row_to_insert: Row
}

impl Statement{
    fn new() -> Statement {
        Statement{
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
            input_length: 0
         }
    }
}
#[derive(Debug)]
struct Table {
    num_rows: usize,
    pages: [Option<[u8; PAGE_SIZE]>; TABLE_MAX_PAGES],
}
impl Table{
    fn new() -> Self{
        Table{
            num_rows: 0,
            pages: [None; TABLE_MAX_PAGES]
        }
    }
    fn row_slot(&mut self, row_id: usize) -> Option<&mut [u8]>{
        let page_num = row_id / ROWS_PER_PAGE;
        if page_num >= TABLE_MAX_PAGES{
            return None;
        }
        let row_offset = row_id  % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        if self.pages[page_num].is_none() {
            self.pages[page_num] = Some([0; PAGE_SIZE]);
        }
        let page = self.pages[page_num].as_mut().unwrap();
        Some(&mut page[byte_offset..byte_offset + ROW_SIZE])
    }
}
fn main() {
    let mut table = Table::new();
    loop {
        let mut input_buffer = InputBuffer::new();
        read_input(&mut input_buffer);
        match do_meta_command(&input_buffer){
            MetaCommandResult::MetaCommandSuccess => {
                break;
            },
            MetaCommandResult::MetaCommandUnrecognizedCommand => {

            },
            MetaCommandResult::MetaNoCommand => println!("No command is selected")
        }
        let mut statement = Statement::new();
        match prepare_statement(&input_buffer, &mut statement) {
            PrepareResult::PrepareSuccess => {
                println!("Prepare success {:?}", statement);
            },
            PrepareResult::PrepareUnrecognizedStatement => {
                println!("Unrecognized keyword at start of {:?}", &input_buffer.buffer.clone());
            }
            PrepareResult::PrepareSyntaxError => {
                println!("Syntax error: could not parse statement");
                continue;
            }
        }
        match execute_statement(&mut statement, &mut table){
            ExecuteSuccess => {
                println!("Query executed successfully");
            }
            ExecuteResult::ExecuteTableFull => {
                println!("Insert is not allowed, Table is full");
                continue;
            }
            ExecuteResult::ExecuteFail => {
                println!("Query execution failed");
                continue;
            }
        }

    }
}
fn print_prompt() {
    println!("db -> ");
}
fn read_input(buffer: &mut InputBuffer) {
    let mut input = String::new();
    print_prompt();
    let n = io::stdin().read_line(&mut input).unwrap();
    if n == 1 {
        buffer.buffer = None;
    }else{
        buffer.input_length = n as i32 - 1;
        buffer.buffer = Some(input.trim_end().to_owned());
    }
}
fn do_meta_command(input_buffer: &InputBuffer) -> MetaCommandResult{
    if let Some(buffer_data) = &input_buffer.buffer{
        if buffer_data.eq(".exit"){
            MetaCommandResult::MetaCommandSuccess
        }else{
            MetaCommandResult::MetaCommandUnrecognizedCommand
        }
    }else{
        MetaCommandResult::MetaNoCommand
    }
}

fn prepare_statement(input_buffer: &InputBuffer, statement: &mut Statement) -> PrepareResult{
    if let Some(buffer_data) = &input_buffer.buffer {
        return match &buffer_data[..6] {
            "insert" => {
                statement.statement_type = Some(StatementType::StatementInsert);
                match scan_fmt!(buffer_data, "insert {} {} {}", i32, String, String) {
                    Ok((id, name, email)) => {
                        statement.row_to_insert.id = id;
                        statement.row_to_insert.email = email;
                        statement.row_to_insert.username = name;
                        PrepareResult::PrepareSuccess
                    }
                    Err(_) => {
                        PrepareResult::PrepareSyntaxError
                    }
                }
            },
            "select" => {
                statement.statement_type = Some(StatementType::StatementSelect);
                PrepareResult::PrepareSuccess
            },
            _ => PrepareResult::PrepareUnrecognizedStatement
        }
    }
    PrepareResult::PrepareUnrecognizedStatement
}

fn execute_statement(statement: &Statement, table: &mut Table) -> ExecuteResult{
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
    }
}
fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult{
    if table.num_rows >= TABLE_MAX_ROWS {
        return ExecuteResult::ExecuteTableFull;
    }
    serialize_row(&statement.row_to_insert, table.row_slot(table.num_rows).unwrap());
    table.num_rows += 1;

    ExecuteSuccess
}
fn execute_select(statement: &Statement, table: &mut Table) -> ExecuteResult{
    let mut row = Row::new();
    for i in 0..table.num_rows{
        deserialize_row(table.row_slot(i).unwrap(), &mut row);
        println!("Row {} {:?}", i, row);
    }
    ExecuteSuccess
}

fn serialize_row(source: &Row, destination: &mut [u8]){
    unsafe {
        ptr::copy_nonoverlapping(
            &source.id as *const i32 as *const u8,
            destination.as_mut_ptr().add(ID_OFFSET),
            ID_SIZE
        );
        let username_bytes = source.username.as_bytes();
        ptr::copy_nonoverlapping(
            username_bytes.as_ptr(),
            destination.as_mut_ptr().add(USERNAME_OFFSET),
            USERNAME_SIZE
        );
        let email_bytes = source.email.as_bytes();
        let email_length = email_bytes.len().min(EMAIL_SIZE);
        ptr::copy_nonoverlapping(
            email_bytes.as_ptr(),
            destination.as_mut_ptr().add(EMAIL_OFFSET),
            email_length
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

        // Copy Email
        let email_bytes = &source[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE];
        destination.email = String::from_utf8_lossy(email_bytes).trim_end_matches('\0').to_string();
    }
}