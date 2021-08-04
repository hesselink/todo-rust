pub mod typed_query {
    use postgres::types::private::BytesMut;
    use postgres::types::{IsNull, Type};
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

    pub struct Insert<C, R: FromRow> {
        table: Table<C, R>,
        values: InsertParams,
    }

    #[derive(Debug)]
    pub enum WithDefault<T> {
        Value(T),
        Default,
    }

    impl<T> postgres::types::ToSql for WithDefault<T>
    where
        T: postgres::types::ToSql + std::fmt::Debug,
    {
        fn to_sql(
            &self,
            ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<(dyn std::error::Error + Send + Sync + 'static)>> {
            match self {
                WithDefault::Value(v) => v.to_sql(ty, out),
                // Should be filtered out of the list of params instead, since we can't send the
                // default value as a parameter value with the libpq format.
                WithDefault::Default => panic!("should never write a default value to sql"),
            }
        }
        fn accepts(ty: &Type) -> bool {
            T::accepts(ty)
        }

        postgres::types::to_sql_checked!();
    }

    pub struct InsertParams(Vec<Vec<Param>>);

    pub trait IsParam: postgres::types::ToSql + IsDefault {
        fn as_dyn_to_sql(&self) -> &(dyn postgres::types::ToSql + Sync);
    }

    impl<T: Sized + postgres::types::ToSql + IsDefault + Sync> IsParam for T {
        fn as_dyn_to_sql(&self) -> &(dyn postgres::types::ToSql + Sync) {
            self
        }
    }

    #[derive(Debug)]
    pub struct Param(pub Box<dyn IsParam + Sync>);

    pub fn insert_into<C, R: FromRow>(table: Table<C, R>) -> Insert<C, R> {
        Insert {
            table,
            values: InsertParams(Vec::new()),
        }
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

    impl<C, R: FromRow> Insert<C, R> {
        pub fn values<V: ToSqlParams>(self, v: V) -> Self {
            let vs = v.to_sql_params();
            let InsertParams(mut values) = self.values;
            values.push(vs);
            Insert {
                table: self.table,
                values: InsertParams(values),
            }
        }

        pub fn execute(&self, client: &mut Client) {
            let q = &self.to_sql();
            let InsertParams(vss) = &self.values;
            let mut ps: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::new();
            for vs in vss {
                for Param(v) in vs {
                    if !(*v).is_default() {
                        let v_: &(dyn postgres::types::ToSql + Sync) = (&**v).as_dyn_to_sql();
                        ps.push(v_);
                    }
                }
            }
            client.execute(q.as_str(), &*ps).unwrap();
        }
    }

    pub trait ToSql {
        fn to_sql(&self) -> String;
    }

    impl<C, R: FromRow> ToSql for Table<C, R> {
        fn to_sql(&self) -> String {
            self.name.to_string()
        }
    }

    impl<C, R: FromRow> ToSql for Query<C, R> {
        fn to_sql(&self) -> String {
            match self {
                Query::Table { table } => format!(
                    // TODO column names
                    "select * from {}",
                    table.to_sql()
                ),
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

    impl<C, R: FromRow> ToSql for Insert<C, R> {
        fn to_sql(&self) -> String {
            "insert into ".to_string() + &self.table.to_sql() + " values " + &self.values.to_sql()
        }
    }

    impl ToSql for InsertParams {
        fn to_sql(&self) -> String {
            let InsertParams(vss) = self;
            let mut ix = 1;
            let mut sql_str = String::new();
            for (i, vs) in vss.iter().enumerate() {
                if i > 0 {
                    sql_str.push_str(", ")
                }
                sql_str.push_str("(");
                for (j, Param(v)) in vs.iter().enumerate() {
                    if j > 0 {
                        sql_str.push_str(", ");
                    }
                    if (**v).is_default() {
                        sql_str.push_str("default");
                    } else {
                        sql_str.push_str("$");
                        sql_str.push_str(&ix.to_string());
                        ix += 1;
                    }
                }
                sql_str.push_str(")");
            }
            sql_str
        }
    }

    pub trait IsDefault {
        fn is_default(&self) -> bool {
            false
        }
    }

    impl<T> IsDefault for WithDefault<T> {
        fn is_default(&self) -> bool {
            match self {
                WithDefault::Value(_) => false,
                WithDefault::Default => true,
            }
        }
    }

    impl IsDefault for String {}
    impl<T> IsDefault for Option<T> {}

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

    pub trait ToSqlParams {
        fn to_sql_params(self) -> Vec<Param>;
    }

    impl<T> SomeField for Field<T> {}

    impl<T> SomeField for &Field<T> {}

    pub struct Constant<T> {
        pub value: T,
    }

    impl<T: ToString> ToSql for Constant<T> {
        fn to_sql(&self) -> String {
            self.value.to_string() // TODO escaping/query params
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

    pub fn asc<F: 'static>(field: &F) -> Order
    where
        F: SomeField + Clone,
    {
        Order {
            by: Box::new((*field).clone()),
            direction: Direction::Ascending,
        }
    }

    pub fn desc<F: 'static>(field: &F) -> Order
    where
        F: SomeField + Clone,
    {
        Order {
            by: Box::new((*field).clone()),
            direction: Direction::Descending,
        }
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
