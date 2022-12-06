CREATE TABLE IF NOT EXISTS Tari (
    id          serial       PRIMARY KEY,
    nume_tara   VARCHAR(100) UNIQUE NOT NULL,
    latitudine  REAL         NOT NULL,
    longitudine REAL         NOT NULL
);

CREATE TABLE IF NOT EXISTS Orase (
    id          serial       PRIMARY KEY,
    id_tara     serial       UNIQUE NOT NULL,
    nume_oras   VARCHAR(100) UNIQUE NOT NULL,
    latitudine  REAL         NOT NULL,
    longitudine REAL         NOT NULL,

    CONSTRAINT FK_id_tara FOREIGN KEY(id_tara)
        REFERENCES Tari(id)
);

CREATE TABLE IF NOT EXISTS Temperaturi (
    id        serial    PRIMARY KEY,
    valoare   REAL      NOT NULL,
    timestamp timestamp UNIQUE NOT NULL,
    id_oras   serial    UNIQUE NOT NULL
);
