CREATE DATABASE IF NOT EXISTS streamify;

USE streamify;


-- Music listening events
CREATE TABLE IF NOT EXISTS listen_events
(
    artist String,
    song String,
    duration Float64,

    ts DateTime64(9),

    sessionid Int32,
    auth String,
    level String,
    itemInSession Int32,

    city String,
    zip Int32,
    state String,
    userAgent String,

    lon Float64,
    lat Float64,

    userId Int64,
    lastName String,
    firstName String,
    gender String,

    registration Int64,
    year Int32,

    month UInt8,
    day UInt8,
    hour UInt8
)
ENGINE = MergeTree
ORDER BY (ts, userId);


-- Authentication events
CREATE TABLE IF NOT EXISTS auth_events
(
    ts DateTime64(9),

    sessionId Int32,
    level String,
    itemInSession Int32,

    city String,
    zip Int32,
    state String,
    userAgent String,

    lon Float64,
    lat Float64,

    userId Int32,
    lastName String,
    firstName String,
    gender String,

    registration Int64,
    success Bool,

    year Int32,

    month UInt8,
    day UInt8,
    hour UInt8
)
ENGINE = MergeTree
ORDER BY (ts, userId);


-- Page view events
CREATE TABLE IF NOT EXISTS page_view_events
(
    ts DateTime64(9),

    sessionId Int32,
    page String,
    auth String,
    method String,
    status Int32,

    level String,
    itemInSession Int32,

    city String,
    zip Int32,
    state String,
    userAgent String,

    lon Float64,
    lat Float64,

    userId Int32,
    lastName String,
    firstName String,
    gender String,

    registration Int64,

    artist String,
    song String,
    duration Float64,

    year Int32,

    month UInt8,
    day UInt8,
    hour UInt8
)
ENGINE = MergeTree
ORDER BY (ts, userId);
