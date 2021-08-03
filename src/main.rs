use postgres::{Client, NoTls};
use std::env;
use std::time::SystemTime;

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
            for row in client
                .query(
                    "select id, name from todo where not completed order by created_time asc",
                    &[],
                )
                .unwrap()
            {
                let id: i32 = row.get(0);
                let name: &str = row.get(1);
                println!("{}: {}", id, name);
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
