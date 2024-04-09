use sqlparser::ast::Values;

use crate::storage::NaadanError;

#[inline(always)]
pub fn add_error_msg(result: &mut Vec<Vec<u8>>, err: NaadanError) {
    result.append(&mut vec![format!("Query execution failed: {}", err)
        .as_bytes()
        .to_vec()]);
}

pub fn prepare_query_output(last_res: Option<Values>) -> String {
    let mut query_result = String::new();
    match last_res {
        Some(res) => {
            res.rows.iter().for_each(|r| {
                let rr: Vec<String> = r.iter().map(|val| val.to_string()).collect();
                query_result += rr.join(", ").as_str();
                query_result += "\n";
            });

            if res.rows.len() > 0 {
                query_result += "\n\n";
            }
        }
        None => {}
    }

    query_result
}
