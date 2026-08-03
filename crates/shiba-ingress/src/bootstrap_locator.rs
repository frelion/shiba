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
             FROM shiba_internal.source_binding AS relation_binding
             JOIN pg_catalog.pg_class AS class
               ON class.oid = relation_binding.address_objid
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
             JOIN pg_catalog.pg_index AS identity
               ON identity.indrelid = class.oid
              AND identity.indisprimary AND identity.indisunique
              AND identity.indisvalid AND identity.indisready
              AND identity.indnkeyatts = 1 AND identity.indnatts = 1
              AND identity.indexprs IS NULL AND identity.indpred IS NULL
             JOIN shiba_internal.source_binding AS key_binding
               ON key_binding.source_id = relation_binding.source_id
              AND key_binding.binding_kind = 'column'
              AND key_binding.address_objid = class.oid
              AND key_binding.address_objsubid = (identity.indkey::smallint[])[0]
             JOIN pg_catalog.pg_attribute AS key
               ON key.attrelid = class.oid AND key.attnum = key_binding.address_objsubid
             JOIN shiba_internal.source_binding AS payload_binding
               ON payload_binding.source_id = relation_binding.source_id
              AND payload_binding.binding_kind = 'column'
              AND payload_binding.address_objid = class.oid
              AND payload_binding.address_objsubid <> key_binding.address_objsubid
             JOIN pg_catalog.pg_attribute AS payload
               ON payload.attrelid = class.oid
              AND payload.attnum = payload_binding.address_objsubid
             WHERE relation_binding.source_id = $1
               AND relation_binding.binding_kind = 'relation'",
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
