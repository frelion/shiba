use std::collections::BTreeMap;

use postgres::Transaction;

use crate::IngressError;

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct PublicationSnapshot {
    pub(crate) name: String,
    pub(crate) insert: bool,
    pub(crate) update: bool,
    pub(crate) delete: bool,
    pub(crate) truncate: bool,
    pub(crate) via_root: bool,
    pub(crate) members: BTreeMap<i64, Vec<i16>>,
}

pub(crate) fn load_live(
    transaction: &mut Transaction<'_>,
    publication_oid: i64,
) -> Result<PublicationSnapshot, IngressError> {
    let publication = transaction
        .query_opt(
            "SELECT pubname::text, puballtables, pubinsert, pubupdate,
                    pubdelete, pubtruncate, pubviaroot
             FROM pg_catalog.pg_publication WHERE oid = $1::bigint::oid",
            &[&publication_oid],
        )?
        .ok_or(IngressError::Governance("publication is missing"))?;
    if publication.get::<_, bool>(1)
        || !publication.get::<_, bool>(2)
        || !publication.get::<_, bool>(3)
        || !publication.get::<_, bool>(4)
        || publication.get::<_, bool>(6)
    {
        return Err(IngressError::Governance(
            "publication policy is not admitted",
        ));
    }
    let rows = transaction.query(
        "SELECT member.prrelid::bigint, member.prqual IS NULL,
                CASE WHEN member.prattrs IS NULL THEN ARRAY(
                    SELECT attribute.attnum::smallint
                    FROM pg_catalog.pg_attribute AS attribute
                    WHERE attribute.attrelid = member.prrelid
                      AND attribute.attnum > 0 AND NOT attribute.attisdropped
                    ORDER BY attribute.attnum
                ) ELSE ARRAY(
                    SELECT published.attnum::smallint
                    FROM pg_catalog.unnest(member.prattrs::smallint[]) AS published(attnum)
                    ORDER BY published.attnum
                ) END
         FROM pg_catalog.pg_publication_rel AS member
         WHERE member.prpubid = $1::bigint::oid ORDER BY member.prrelid",
        &[&publication_oid],
    )?;
    if rows.is_empty() || rows.len() > 2 || rows.iter().any(|row| !row.get::<_, bool>(1)) {
        return Err(IngressError::Governance(
            "publication membership is not admitted",
        ));
    }
    let members = rows
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Vec<i16>>(2)))
        .collect();
    Ok(PublicationSnapshot {
        name: publication.get(0),
        insert: publication.get(2),
        update: publication.get(3),
        delete: publication.get(4),
        truncate: publication.get(5),
        via_root: publication.get(6),
        members,
    })
}
