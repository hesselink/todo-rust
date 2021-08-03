use postgres::{Client, NoTls, Row};
use std::env;
use std::marker::PhantomData;
use std::time::SystemTime;

use todo_rust::typed_query;

fn main() {
    let mut client =
        Client::connect("host=localhost user=postgres password=postgres", NoTls).unwrap();
    create_tables(&mut client);

    let args = env::args();
    let command = parse_args(args);
    match command {
        Err(e) => {
            println!("{}", e);
            print_usage();
        }
        Ok(cmd) => {
            run_command(&mut client, cmd);
        }
    }
}

struct TodoRecord {
    id: i32,
    name: String,
    created_time: SystemTime,
    completed: bool,
    completed_time: SystemTime,
}

impl typed_query::FromRow for TodoRecord {
    fn from_row(row: Row) -> TodoRecord {
        TodoRecord {
            id: row.get(0),
            name: row.get(1),
            created_time: row.get(2),
            completed: row.get(3),
            completed_time: row.get(4),
        }
    }
}

const TODO_TABLE: typed_query::Table<TodoColumns, TodoRecord> = typed_query::Table {
    name: "todo",
    columns: TodoColumns {
        id: typed_query::Field {
            name: "id",
            phantom: PhantomData,
        },
        name: typed_query::Field {
            name: "name",
            phantom: PhantomData,
        },
        created_time: typed_query::Field {
            name: "created_time",
            phantom: PhantomData,
        },
        completed: typed_query::Field {
            name: "completed",
            phantom: PhantomData,
        },
        completed_time: typed_query::Field {
            name: "completed_time",
            phantom: PhantomData,
        },
    },
    phantom: PhantomData,
};

struct TodoColumns {
    id: typed_query::Field<i32>,
    name: typed_query::Field<String>,
    created_time: typed_query::Field<SystemTime>,
    completed: typed_query::Field<bool>,
    completed_time: typed_query::Field<SystemTime>,
}

fn create_tables(client: &mut Client) {
    client
        .execute(
            "
        create table if not exists todo (
            id serial primary key,
            name text not null,
            created_time timestamp with time zone not null default now(),
            completed boolean not null default false,
            completed_time timestamp with time zone null
        )",
            &[],
        )
        .unwrap();
}

#[derive(Debug)]
enum Command {
    Add { name: String },
    List,
    Complete { id: i32 },
}

fn parse_args(mut args: env::Args) -> Result<Command, String> {
    match args.nth(1) {
        Some(s) => match s.as_str() {
            "add" => args
                .nth(0)
                .map(|arg| Command::Add { name: arg })
                .ok_or("Missing argument to 'add' command".to_string()),
            "list" => Ok(Command::List),
            "complete" => {
                let id_str = args
                    .nth(0)
                    .ok_or("Missing argument to 'complete' command".to_string())?;
                let id = id_str.parse::<i32>().map_err(|e| {
                    format!("Failed to parse argument as number: {}, {}", id_str, e)
                })?;
                Ok(Command::Complete { id })
            }
            cmd => Err(format!("Unknown command: {}", cmd)),
        },
        None => Err("No command found".to_string()),
    }
}

fn print_usage() {
    // TODO
}

fn run_command(client: &mut Client, command: Command) {
    match command {
        Command::Add { name } => {
            client
                .execute("insert into todo (name) values ($1)", &[&name])
                .unwrap();
        }
        Command::List => {
            for row in typed_query::from(TODO_TABLE)
                .where_(|t| {
                    t.completed
                        .clone()
                        .eq(typed_query::Constant { value: false })
                })
                .order_by(|t| typed_query::asc(&t.created_time))
                .query(client)
            {
                println!("{}: {}", row.id, row.name);
            }
        }
        Command::Complete { id } => {
            client
                .execute(
                    "update todo set completed = true, completed_time = $1 where id = $2",
                    &[&SystemTime::now(), &id],
                )
                .unwrap();
        }
    }
}
