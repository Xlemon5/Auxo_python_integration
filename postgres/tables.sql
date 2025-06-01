CREATE SCHEMA IF NOT EXISTS spaceX;

CREATE TABLE IF NOT EXISTS spaceX.crew (
    name text,
    agency text,
    image text,
    wikipedia text,
    launches text[],
    status text,
    id text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS spaceX.cores (
	block numeric NULL,
	reuse_count numeric NULL,
	rtls_attempts numeric NULL,
	rtls_landings numeric NULL,
	asds_attempts numeric NULL,
	asds_landings numeric NULL,
	last_update text NULL,
	launches text[] NULL,
	serial text NULL,
	status text NULL,
	id text NULL PRIMARY KEY
);