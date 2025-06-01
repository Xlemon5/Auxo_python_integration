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