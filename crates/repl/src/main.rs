use std::io;
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
                username: String::new(),
                email: String::new(),
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