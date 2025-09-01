use pgrx::prelude::*;
use pgrx::spi::{self, Spi};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tpchgen::{
    csv::{
        CustomerCsv, LineItemCsv, NationCsv, OrderCsv, PartCsv, PartSuppCsv, RegionCsv, SupplierCsv,
    },
    generators::{
        CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
        PartSuppGenerator, RegionGenerator, SupplierGenerator,
    },
};

::pgrx::pg_module_magic!(name, version);

extension_sql!(
    r#"
    CREATE TABLE IF NOT EXISTS region (
        r_regionkey integer NOT NULL,
        r_name character(25) NOT NULL,
        r_comment character varying(152)
    );
    CREATE TABLE IF NOT EXISTS nation (
        n_nationkey integer NOT NULL,
        n_name character(25) NOT NULL,
        n_regionkey integer NOT NULL,
        n_comment character varying(152)
    );
    CREATE TABLE IF NOT EXISTS part (
        p_partkey integer NOT NULL,
        p_name character varying(55) NOT NULL,
        p_mfgr character(25) NOT NULL,
        p_brand character(10) NOT NULL,
        p_type character varying(25) NOT NULL,
        p_size integer NOT NULL,
        p_container character(10) NOT NULL,
        p_retailprice numeric(15,2) NOT NULL,
        p_comment character varying(23) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS supplier (
        s_suppkey integer NOT NULL,
        s_name character(25) NOT NULL,
        s_address character varying(40) NOT NULL,
        s_nationkey integer NOT NULL,
        s_phone character(15) NOT NULL,
        s_acctbal numeric(15,2) NOT NULL,
        s_comment character varying(101) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS partsupp (
        ps_partkey integer NOT NULL,
        ps_suppkey integer NOT NULL,
        ps_availqty integer NOT NULL,
        ps_supplycost numeric(15,2) NOT NULL,
        ps_comment character varying(199) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS customer (
        c_custkey integer NOT NULL,
        c_name character varying(25) NOT NULL,
        c_address character varying(40) NOT NULL,
        c_nationkey integer NOT NULL,
        c_phone character(15) NOT NULL,
        c_acctbal numeric(15,2) NOT NULL,
        c_mktsegment character(10) NOT NULL,
        c_comment character varying(117) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS orders (
        o_orderkey integer NOT NULL,
        o_custkey integer NOT NULL,
        o_orderstatus character(1) NOT NULL,
        o_totalprice numeric(15,2) NOT NULL,
        o_orderdate date NOT NULL,
        o_orderpriority character(15) NOT NULL,
        o_clerk character(15) NOT NULL,
        o_shippriority integer NOT NULL,
        o_comment character varying(79) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS lineitem (
        l_orderkey integer NOT NULL,
        l_partkey integer NOT NULL,
        l_suppkey integer NOT NULL,
        l_linenumber integer NOT NULL,
        l_quantity numeric(15,2) NOT NULL,
        l_extendedprice numeric(15,2) NOT NULL,
        l_discount numeric(15,2) NOT NULL,
        l_tax numeric(15,2) NOT NULL,
        l_returnflag character(1) NOT NULL,
        l_linestatus character(1) NOT NULL,
        l_shipdate date NOT NULL,
        l_commitdate date NOT NULL,
        l_receiptdate date NOT NULL,
        l_shipinstruct character(25) NOT NULL,
        l_shipmode character(10) NOT NULL,
        l_comment character varying(44) NOT NULL
    );
    "#,
    name = "create_schema"
);

mod queries;

const TPCH_DATA_DIR: &str = "/tmp/pg_tpch_data";

fn truncate_tables() -> spi::Result<()> {
    Spi::run(
        r#"
    TRUNCATE TABLE region, nation, part, supplier, partsupp, customer, orders, lineitem RESTART IDENTITY;
    "#,
    )
}

#[pg_extern]
fn tpch_load(
    sf: default!(f64, 1.),
    children: default!(i64, 1),
    step: default!(i64, 0),
) -> spi::Result<Option<String>> {
    if sf == 0. {
        truncate_tables()?;
        return Ok(Some("TPC-H tables truncated".to_string()));
    }

    if children < 1 || step < 0 || step >= children {
        return Err(spi::SpiError::PreparedStatementArgumentMismatch {
            expected: children as usize,
            got: step as usize,
        });
    }

    if step == 0 {
        truncate_tables()?;
    }

    let part = (step + 1) as i32;
    let num_parts = children as i32;

    macro_rules! generate_and_copy_csv_table {
        ($table_name:expr, $generator:expr, $csv_formatter:ty) => {
            || -> spi::Result<()> {
                let dir = PathBuf::from(TPCH_DATA_DIR);
                fs::create_dir_all(&dir).unwrap();

                let file_path = dir.join(format!("{}.csv", $table_name));
                let mut file = fs::File::create(&file_path).unwrap();

                // Write header
                writeln!(&mut file, "{}", <$csv_formatter>::header()).unwrap();

                // Write rows
                for item in $generator {
                    writeln!(&mut file, "{}", <$csv_formatter>::new(item)).unwrap();
                }

                let absolute_file_path = fs::canonicalize(&file_path).unwrap();

                let copy_query = format!(
                    "COPY {} FROM '{}' WITH (FORMAT csv, HEADER true, DELIMITER ',')",
                    $table_name,
                    absolute_file_path.display()
                );

                Spi::run(&copy_query)?;

                fs::remove_file(&file_path).unwrap();

                Ok(())
            }()
        };
    }

    generate_and_copy_csv_table!(
        "region",
        RegionGenerator::new(sf, part, num_parts),
        RegionCsv
    )?;
    generate_and_copy_csv_table!(
        "nation",
        NationGenerator::new(sf, part, num_parts),
        NationCsv
    )?;
    generate_and_copy_csv_table!("part", PartGenerator::new(sf, part, num_parts), PartCsv)?;
    generate_and_copy_csv_table!(
        "supplier",
        SupplierGenerator::new(sf, part, num_parts),
        SupplierCsv
    )?;
    generate_and_copy_csv_table!(
        "partsupp",
        PartSuppGenerator::new(sf, part, num_parts),
        PartSuppCsv
    )?;
    generate_and_copy_csv_table!(
        "customer",
        CustomerGenerator::new(sf, part, num_parts),
        CustomerCsv
    )?;
    generate_and_copy_csv_table!("orders", OrderGenerator::new(sf, part, num_parts), OrderCsv)?;
    generate_and_copy_csv_table!(
        "lineitem",
        LineItemGenerator::new(sf, part, num_parts),
        LineItemCsv
    )?;

    Ok(Some(format!(
        "TPC-H SF={} loaded (part {}/{})",
        sf,
        step + 1,
        children
    )))
}

#[pg_extern]
fn tpch_queries() -> Vec<String> {
    queries::QUERIES
        .iter()
        .map(|(nr, q)| format!("query_nr: {}, query: {}", nr, q))
        .collect()
}

#[pg_extern]
fn tpch_query(query_nr: i32) -> spi::Result<String> {
    let query = queries::QUERIES
        .iter()
        .find(|query| query.0 == query_nr)
        .expect("Invalid query number must be between 1 and 22 (inclusive)");
    Ok(query.1.to_string())
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_tpch_load_truncate() {
        let result = crate::tpch_load(0.0, 1, 0).unwrap();
        assert_eq!(result, Some("TPC-H tables truncated".to_string()));
    }

    #[pg_test]
    fn test_tpch_queries() {
        let results = crate::tpch_queries();
        assert_eq!(results.len(), 22);
    }

    #[pg_test]
    fn test_tpch_query_returns_string() {
        let query_text = crate::tpch_query(1).unwrap();
        println!("{}", query_text);
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
