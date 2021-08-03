pub mod typed_query {
    use postgres::{Client, Row};
    use std::marker::PhantomData;

    pub trait FromRow {
        fn from_row(row: Row) -> Self;
    }

    pub struct Table<C, R: FromRow> {
        pub name: &'static str,
        pub columns: C,
        pub phantom: PhantomData<R>,
    }

    pub struct Field<T> {
        pub name: &'static str,
        pub phantom: PhantomData<T>,
    }

    pub struct Column {
        pub name: &'static str,
    }

    pub enum Query<C, R: FromRow> {
        Table {
            table: Table<C, R>,
        },
        Where {
            query: Box<Query<C, R>>,
            predicate: Predicate,
        },
        Order {
            query: Box<Query<C, R>>,
            order: Order,
        },
    }

    pub fn from<C, R: FromRow>(table: Table<C, R>) -> Query<C, R> {
        Query::Table { table }
    }

    impl<C, R: FromRow> Query<C, R> {
        pub fn columns(&self) -> &C {
            match self {
                Query::Table { table } => &table.columns,
                Query::Where {
                    query,
                    predicate: _,
                } => query.columns(),
                Query::Order { query, order: _ } => query.columns(),
            }
        }

        pub fn query(&self, client: &mut Client) -> Vec<R> {
            let mut vec: Vec<R> = Vec::new();

            let q = &self.to_sql();
            print!("Executing query: {}", q);
            for row in client.query(q.as_str(), &[]).unwrap() {
                vec.push(FromRow::from_row(row));
            }
            vec
        }

        pub fn where_<F>(self, condition: F) -> Query<C, R>
        where
            F: FnOnce(&C) -> Predicate + Sized,
        {
            let predicate = condition(self.columns());
            Query::Where {
                query: Box::new(self),
                predicate: predicate,
            }
        }

        pub fn order_by<F>(self, make_order: F) -> Query<C, R>
        where
            F: FnOnce(&C) -> Order + Sized,
        {
            let order = make_order(self.columns());
            Query::Order {
                query: Box::new(self),
                order: order,
            }
        }
    }

    pub trait ToSql {
        fn to_sql(&self) -> String;
    }

    impl<C, R: FromRow> ToSql for Table<C, R> {
        fn to_sql(&self) -> String {
            format!("select * from {}", self.name) // TODO column names
        }
    }

    impl<C, R: FromRow> ToSql for Query<C, R> {
        fn to_sql(&self) -> String {
            match self {
                Query::Table { table } => table.to_sql(),
                Query::Where { query, predicate } => format!(
                    "select * from ({}) t where {}", // TODO unique number on alias
                    query.to_sql(),
                    predicate.to_sql()
                ),
                Query::Order { query, order } => format!(
                    "select * from ({}) t order by {}", // TODO unique number on alias
                    query.to_sql(),
                    order.to_sql()
                ),
            }
        }
    }

    pub trait SomeField: ToSql {}

    impl<T> ToSql for Field<T> {
        fn to_sql(&self) -> String {
            self.name.to_string()
        }
    }

    impl<T> ToSql for &Field<T> {
        fn to_sql(&self) -> String {
            self.name.to_string()
        }
    }

    impl<T> SomeField for Field<T> {}

    impl<T> SomeField for &Field<T> {}

    pub struct Constant<T> {
        pub value: T,
    }

    impl<T: ToString> ToSql for Constant<T> {
        fn to_sql(&self) -> String {
            self.value.to_string()
        }
    }

    impl<T: ToString> SomeField for Constant<T> {}

    pub enum Predicate {
        Eq {
            field1: Box<dyn SomeField>,
            field2: Box<dyn SomeField>,
        },
    }

    impl ToSql for Predicate {
        fn to_sql(&self) -> String {
            match self {
                Predicate::Eq { field1, field2 } => field1.to_sql() + " = " + &field2.to_sql(),
            }
        }
    }

    impl<T: 'static> Field<T> {
        pub fn eq(self, other: impl SomeField + 'static) -> Predicate {
            Predicate::Eq {
                field1: Box::new(self),
                field2: Box::new(other),
            }
        }
    }

    impl<T: Clone> Clone for Field<T> {
        fn clone(&self) -> Self {
            Field {
                name: &self.name,
                phantom: PhantomData,
            }
        }
    }

    pub enum Direction {
        Ascending,
        Descending,
    }

    pub struct Order {
        pub by: Box<dyn SomeField>,
        pub direction: Direction,
    }

    pub fn asc<F: 'static>(field: &F) -> Order where F: SomeField + Clone {
        Order { by: Box::new((*field).clone()), direction: Direction::Ascending }
    }

    pub fn desc<F: 'static>(field: &F) -> Order where F: SomeField + Clone {
        Order { by: Box::new((*field).clone()), direction: Direction::Descending }
    }

    impl ToSql for Order {
        fn to_sql(&self) -> String {
            self.by.to_sql() + " " + &self.direction.to_sql()
        }
    }

    impl ToSql for Direction {
        fn to_sql(&self) -> String {
            match self {
                Direction::Ascending => "asc".to_string(),
                Direction::Descending => "desc".to_string(),
            }
        }
    }
}
