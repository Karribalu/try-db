use std::borrow::ToOwned;
use std::clone::Clone;
use std::{io, ptr};
use std::mem::size_of;
use scan_fmt::scan_fmt;

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

const ID_SIZE: usize = size_of::<i32>();
const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
/*
+void serialize_row(Row* source, void* destination) {
+  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
+  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
+  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
+}
+
+void deserialize_row(void* source, Row* destination) {
+  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
+  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
+  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
+}
 */
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
fn main() {
    loop {
        let mut input_buffer = InputBuffer::new();
        read_input(&mut input_buffer);
        match do_meta_command(&input_buffer){
            MetaCommandResult::MetaCommandSuccess => {
                break;
            },
            MetaCommandResult::MetaCommandUnrecognizedCommand => println!("Unrecognized command {}", &input_buffer.buffer.clone().unwrap()),
            MetaCommandResult::MetaNoCommand => println!("No command is selected")
        }
        let mut statement = Statement::new();
        match prepare_statement(&input_buffer, &mut statement) {
            PrepareResult::PrepareSuccess => {
                println!("Prepare success {:?}", statement);
                let mut buffer: Vec<u8> = vec![0; ID_SIZE + USERNAME_SIZE + EMAIL_SIZE];

                serialize_row(&statement.row_to_insert, &mut buffer);
                println!("serialization {:?}", buffer);
                let mut destination_row = Row::new();

                deserialize_row(&buffer, &mut destination_row);
                println!("deserialization {:?}", destination_row);
                assert_eq!(statement.row_to_insert.id, destination_row.id);
                assert_eq!(&statement.row_to_insert.username, &destination_row.username);
                assert_eq!(&statement.row_to_insert.email, &destination_row.email);
            },
            PrepareResult::PrepareUnrecognizedStatement => {
                println!("Unrecognized keyword at start of {:?}", &input_buffer.buffer.clone());
            }
            _ => {}
        }
        execute_statement(&mut statement);
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

fn execute_statement(statement: &mut Statement){
    match &statement.statement_type {
        None => {
            println!("The statement is not valid for execution");
        }
        Some(stmt) => {
            match stmt {
                StatementType::StatementInsert => {
                    println!("Insert statement is being executed");
                }
                StatementType::StatementSelect => {
                    println!("Select statement is being executed");
                }
            }
        }
    }
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