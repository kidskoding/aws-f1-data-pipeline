-- Warehouse schema
CREATE SCHEMA IF NOT EXISTS f1;

-- Drivers: small dimension, copied to every compute slice so joins never move data
CREATE TABLE IF NOT EXISTS f1.drivers (
  driver_id         VARCHAR(64) NOT NULL,
  code              VARCHAR(8),
  permanent_number  INTEGER,
  given_name        VARCHAR(64),
  family_name       VARCHAR(64),
  date_of_birth     DATE,
  nationality       VARCHAR(32),
  PRIMARY KEY (driver_id)
) DISTSTYLE ALL;

-- Constructors (teams)
CREATE TABLE IF NOT EXISTS f1.constructors (
  constructor_id  VARCHAR(64) NOT NULL,
  name            VARCHAR(64),
  nationality     VARCHAR(32),
  PRIMARY KEY (constructor_id)
) DISTSTYLE ALL;

-- Circuits
CREATE TABLE IF NOT EXISTS f1.circuits (
  circuit_id    VARCHAR(64) NOT NULL,
  circuit_name  VARCHAR(128),
  locality      VARCHAR(64),
  country       VARCHAR(64),
  lat           DOUBLE PRECISION,
  lng           DOUBLE PRECISION,
  PRIMARY KEY (circuit_id)
) DISTSTYLE ALL;

-- Races: one row per Grand Prix weekend
CREATE TABLE IF NOT EXISTS f1.races (
  season      INTEGER NOT NULL,
  round       INTEGER NOT NULL,
  race_name   VARCHAR(128),
  race_date   DATE,
  circuit_id  VARCHAR(64),
  PRIMARY KEY (season, round)
) DISTSTYLE ALL SORTKEY (season, round);

-- Results: the fact table, one row per driver per race or sprint.
-- Column order must match the Parquet files, because COPY maps Parquet columns by position.
CREATE TABLE IF NOT EXISTS f1.results (
  session              VARCHAR(8)  NOT NULL,
  season               INTEGER     NOT NULL,
  round                INTEGER     NOT NULL,
  driver_id            VARCHAR(64) NOT NULL,
  constructor_id       VARCHAR(64),
  car_number           INTEGER,
  grid                 INTEGER,
  position             INTEGER,
  position_text        VARCHAR(4),
  classified           BOOLEAN,
  points               DECIMAL(6,2),
  laps                 INTEGER,
  status               VARCHAR(64),
  finish_category      VARCHAR(16),
  race_time_ms         BIGINT,
  fastest_lap_rank     INTEGER,
  fastest_lap_time_ms  INTEGER,
  PRIMARY KEY (session, season, round, driver_id)
) DISTKEY (driver_id) SORTKEY (season, round);

-- Official standings from the API, kept to reconcile our computed points against
CREATE TABLE IF NOT EXISTS f1.official_standings (
  season          INTEGER NOT NULL,
  round           INTEGER,
  driver_id       VARCHAR(64) NOT NULL,
  constructor_id  VARCHAR(64),
  position        INTEGER,
  points          DECIMAL(6,2),
  wins            INTEGER,
  PRIMARY KEY (season, driver_id)
) DISTSTYLE ALL;
