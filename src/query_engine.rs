use crate::utils::log;

#[derive(Debug)]
pub struct DBQuery {
    query_string: String,
    params: Vec<String>,
}

impl DBQuery {
    pub fn new(query: String) -> Self {
        Self {
            query_string: query,
            params: vec![],
        }
    }

    pub fn add_param(&mut self, param1: String) -> &mut Self {
        self.params.push(param1);
        self
    }

    pub fn execute(&mut self) -> Result<bool, bool> {
        let log_string = format!("Executing script {:?} {:?}", self.params, self.query_string);
        log(log_string);

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut query = DBQuery::new("select * from table".to_string());

        query.add_param("name".to_string());
        query.add_param("query2".to_string());
        let query_status = query.execute();

        let log_string = format!("Query status {:?}", query_status);
        log(log_string);
    }
}
