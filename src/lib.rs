use log::info;
use rsheet_lib::cell_value::CellValue;
use rsheet_lib::cells::{column_name_to_number, column_number_to_name};
use rsheet_lib::command_runner::{CellArgument, CommandRunner};
use rsheet_lib::connect::{Manager, Reader, Writer};
use rsheet_lib::replies::Reply;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};

struct Coordinator {
    expressions: Arc<Mutex<HashMap<String, String>>>,
    cell_values: Arc<Mutex<HashMap<String, CellValue>>>,
    expression_sender: Sender<String>,
}

impl Coordinator {
    fn new(expression_sender: Sender<String>) -> Self {
        Coordinator {
            expressions: Arc::new(Mutex::new(HashMap::new())),
            cell_values: Arc::new(Mutex::new(HashMap::new())),
            expression_sender,
        }
    }

    fn get_cell(&self, cell_name: &str) -> CellValue {
        self.cell_values
            .lock()
            .unwrap()
            .get(cell_name)
            .cloned()
            .unwrap_or(CellValue::None)
    }

    fn set_cell(&self, cell_name: &str, expression: &str) {
        self.expressions
            .lock()
            .unwrap()
            .insert(cell_name.to_string(), expression.to_string());
        let mut visited: HashSet<String> = HashSet::new();
        let value =
            calculate_cell_value(&self.expressions.lock().unwrap(), cell_name, &mut visited);
        self.cell_values
            .lock()
            .unwrap()
            .insert(cell_name.to_owned(), value);
        let _ = self.expression_sender.send(cell_name.to_string());
    }

    fn update_cell_values(&self, the_cell_name: String) {
        let expressions = self.expressions.lock().unwrap().clone();

        for cell_name in expressions.keys() {
            if *cell_name != the_cell_name {
                let mut visited: HashSet<String> = HashSet::new();
                let value = calculate_cell_value(&expressions, cell_name, &mut visited);
                self.cell_values
                    .lock()
                    .unwrap()
                    .insert(cell_name.to_owned(), value);
            }
        }
    }
}

pub fn start_server<M>(mut manager: M) -> Result<(), Box<dyn Error>>
where
    M: Manager,
{
    let (expression_sender, expression_update_receiver) = channel();
    let coordinator = Arc::new(Coordinator::new(expression_sender));

    let coordinator_clone = coordinator.clone();
    std::thread::spawn(move || {
        while let Ok(the_cell_name) = expression_update_receiver.recv() {
            coordinator_clone.update_cell_values(the_cell_name);
        }
    });

    std::thread::scope(|s| loop {
        if let Ok((recv, send)) = manager.accept_new_connection() {
            let coordinator = coordinator.clone();

            s.spawn(move || {
                let _ = handle_connection(recv, send, coordinator);
            });
        } else {
            return Ok(());
        }
    })
}

fn handle_connection<R, W>(
    mut recv: R,
    mut send: W,
    coordinator: Arc<Coordinator>,
) -> Result<(), Box<dyn Error>>
where
    R: Reader,
    W: Writer,
{
    loop {
        info!("Just got message");
        let msg = recv.read_message()?;
        let parts: Vec<&str> = msg.trim().splitn(2, ' ').collect();

        match parts[0] {
            "get" => {
                if parts.len() != 2 {
                    send.write_message(Reply::Error("Invalid get command".to_string()))?
                } else {
                    let cell_name = parts[1];
                    let cell_value = coordinator.get_cell(cell_name);
                    match cell_value {
                        CellValue::String(err) if err == "Runtime error: Unknown value: \"Circular dependency detected\" (line 1, position 1)" => {
                            send.write_message(Reply::Error("Circular dependency".to_string()))?
                        }
                        CellValue::Error(err) if err == "Runtime error: Unknown value: \"Circular dependency detected\" (line 1, position 1)" => {
                            send.write_message(Reply::Error("Circular dependency".to_string()))?
                        }
                        CellValue::String(err) if err == "'this' can only be used in functions (line 1, position 7)" => {
                            send.write_message(Reply::Error("this err".to_string()))?
                        }
                        CellValue::String(err) if err == "Circular dependency detected" => {
                            send.write_message(Reply::Error("Circular dependency".to_string()))?
                        }
                        _ => send.write_message(Reply::Value(cell_name.to_string(), cell_value))?,
                    }
                }
            }
            "set" => {
                if parts.len() != 2 {
                    send.write_message(Reply::Error("Invalid set command".to_string()))?;
                } else {
                    let mut parts = parts[1].splitn(2, ' ');
                    let cell_name = parts.next().unwrap();
                    let expression = parts.next().unwrap_or("");
                    if expression.is_empty() {
                        send.write_message(Reply::Error("Invalid command".to_string()))?
                    } else {
                        coordinator.set_cell(cell_name, expression);
                    }
                }
            }
            _ => send.write_message(Reply::Error("Invalid command".to_string()))?,
        };
    }
}

fn get_vector_value(
    cells: &HashMap<String, CellValue>,
    col_start: u32,
    row_start: u32,
    col_end: u32,
    row_end: u32,
) -> Vec<CellValue> {
    if col_start == col_end {
        (row_start..=row_end)
            .map(|row| get_cell_value(cells, col_start, row))
            .collect()
    } else {
        (col_start..=col_end)
            .map(|col| get_cell_value(cells, col, row_start))
            .collect()
    }
}

fn get_matrix_value(
    cells: &HashMap<String, CellValue>,
    col_start: u32,
    row_start: u32,
    col_end: u32,
    row_end: u32,
) -> Vec<Vec<CellValue>> {
    (row_start..=row_end)
        .map(|row| {
            (col_start..=col_end)
                .map(|col| get_cell_value(cells, col, row))
                .collect()
        })
        .collect()
}

fn get_cell_value(cells: &HashMap<String, CellValue>, col: u32, row: u32) -> CellValue {
    let cell_name = format!("{}{}", column_number_to_name(col), row);
    cells.get(&cell_name).cloned().unwrap_or(CellValue::None)
}

fn calculate_variables(
    expressions: &HashMap<String, String>,
    expression: &str,
    visited: &mut HashSet<String>,
) -> HashMap<String, CellArgument> {
    let command_runner = CommandRunner::new(expression);
    command_runner
        .find_variables()
        .into_iter()
        .map(|var_name| {
            let cell_argument = if var_name.contains('_')
                && (!var_name.contains("sum") || !var_name.contains("sleep"))
            {
                let parts: Vec<&str> = var_name.split('_').collect();
                let start_col = parts[0]
                    .chars()
                    .take_while(|c| c.is_alphabetic())
                    .collect::<String>();
                let start_row = parts[0][start_col.len()..].parse::<u32>().unwrap();
                let end_col = parts[1]
                    .chars()
                    .take_while(|c| c.is_alphabetic())
                    .collect::<String>();
                let end_row = parts[1][end_col.len()..].parse::<u32>().unwrap();
                let col_start = column_name_to_number(&start_col);
                let col_end = column_name_to_number(&end_col);
                let cells = expressions
                    .iter()
                    .map(|(name, _)| {
                        (
                            name.clone(),
                            calculate_cell_value(expressions, name, visited),
                        )
                    })
                    .collect();
                if col_start == col_end || start_row == end_row {
                    let value = get_vector_value(&cells, col_start, start_row, col_end, end_row);
                    CellArgument::Vector(value)
                } else {
                    let value = get_matrix_value(&cells, col_start, start_row, col_end, end_row);
                    CellArgument::Matrix(value)
                }
            } else {
                let value = calculate_cell_value(expressions, &var_name, visited);
                CellArgument::Value(value)
            };
            (var_name.clone(), cell_argument)
        })
        .collect()
}

fn calculate_cell_value(
    expressions: &HashMap<String, String>,
    cell_name: &str,
    visited: &mut HashSet<String>,
) -> CellValue {
    if visited.contains(cell_name) {
        return CellValue::Error("Circular dependency detected".to_string());
    }

    if let Some(expression) = expressions.get(cell_name) {
        visited.insert(cell_name.to_string());
        let variables = calculate_variables(expressions, expression, visited);
        visited.remove(cell_name);

        let command_runner = CommandRunner::new(expression);
        command_runner.run(&variables)
    } else {
        CellValue::None
    }
}
