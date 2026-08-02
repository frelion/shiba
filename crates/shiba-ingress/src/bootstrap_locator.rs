use postgres::Client;

use crate::IngressError;

pub(crate) struct ScanLocator {
    pub(crate) query: String,
}

impl ScanLocator {
    pub(crate) fn load(client: &mut Client, source_id: i64) -> Result<Self, IngressError> {
        let row = client.query_one(
            "SELECT namespace.nspname::text, class.relname::text,
                    key.attname::text, payload.attname::text,
                    key.atttypid::bigint, key.attnotnull,
                    payload.atttypid::bigint, payload.attnotnull
             FROM shiba_internal.source_binding AS binding
             JOIN pg_catalog.pg_class AS class ON class.oid = binding.address_objid
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
             JOIN pg_catalog.pg_attribute AS key
               ON key.attrelid = class.oid AND key.attnum = 1
             JOIN pg_catalog.pg_attribute AS payload
               ON payload.attrelid = class.oid AND payload.attnum = 2
             WHERE binding.source_id = $1 AND binding.binding_kind = 'relation'",
            &[&source_id],
        )?;
        if row.get::<_, i64>(4) != 20
            || !row.get::<_, bool>(5)
            || row.get::<_, i64>(6) != 20
            || row.get::<_, bool>(7)
        {
            return Err(IngressError::Governance(
                "bootstrap requires int8 key and nullable int8 payload",
            ));
        }
        let relation = format!(
            "{}.{}",
            quote_identifier(row.get(0)),
            quote_identifier(row.get(1))
        );
        let key = quote_identifier(row.get(2));
        let payload = quote_identifier(row.get(3));
        Ok(Self {
            query: format!(
                "SELECT {key}, {payload} FROM {relation}
                 WHERE ($1::bigint IS NULL OR {key} > $1)
                 ORDER BY {key} LIMIT $2"
            ),
        })
    }
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
