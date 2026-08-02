-- Clean-room Phase-1 catalog. This file is intentionally self-contained so
-- CREATE EXTENSION is the only transaction needed to install or roll it back.

CREATE SCHEMA shiba_internal;
REVOKE ALL ON SCHEMA shiba_internal FROM PUBLIC;

CREATE SCHEMA shiba;
REVOKE ALL ON SCHEMA shiba FROM PUBLIC;
GRANT USAGE ON SCHEMA shiba TO PUBLIC;

CREATE TABLE shiba_internal.catalog_identity (
    singleton smallint NOT NULL,
    catalog_version integer NOT NULL,
    protocol_version integer NOT NULL,
    CONSTRAINT catalog_identity_primary PRIMARY KEY (singleton),
    CONSTRAINT catalog_identity_singleton CHECK (singleton = 1),
    CONSTRAINT catalog_identity_catalog_version CHECK (catalog_version = 1),
    CONSTRAINT catalog_identity_protocol_version CHECK (protocol_version = 1)
);

INSERT INTO shiba_internal.catalog_identity (
    singleton,
    catalog_version,
    protocol_version
)
VALUES (1, 1, 1);

REVOKE ALL ON TABLE shiba_internal.catalog_identity FROM PUBLIC;

CREATE FUNCTION shiba.versions()
RETURNS TABLE (
    catalog_version integer,
    protocol_version integer
)
LANGUAGE sql
STABLE
PARALLEL SAFE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT identity.catalog_version, identity.protocol_version
    FROM shiba_internal.catalog_identity AS identity
    WHERE identity.singleton = 1
$function$;

REVOKE ALL ON FUNCTION shiba.versions() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION shiba.versions() TO PUBLIC;

COMMENT ON SCHEMA shiba_internal IS
    'Private Shiba V2 extension state; no public data access contract';
COMMENT ON SCHEMA shiba IS
    'Read-only public API for Shiba V2 installation metadata';
COMMENT ON TABLE shiba_internal.catalog_identity IS
    'Single database-local authority for clean-room catalog and protocol versions';
COMMENT ON FUNCTION shiba.versions() IS
    'Return the installed clean-room catalog and protocol versions';
