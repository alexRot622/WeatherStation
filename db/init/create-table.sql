CREATE TABLE IF NOT EXISTS Tari (
    id          serial       PRIMARY KEY,
    nume_tara   VARCHAR(100) UNIQUE NOT NULL,
    latitudine  REAL         NOT NULL,
    longitudine REAL         NOT NULL
);

CREATE TABLE IF NOT EXISTS Orase (
    id          serial       PRIMARY KEY,
    id_tara     serial       NOT NULL,
    nume_oras   VARCHAR(100) NOT NULL,
    latitudine  REAL         NOT NULL,
    longitudine REAL         NOT NULL,

    UNIQUE (id_tara, nume_oras),
    CONSTRAINT FK_id_tara FOREIGN KEY(id_tara)
        REFERENCES Tari(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS Temperaturi (
    id        serial    PRIMARY KEY,
    valoare   REAL      NOT NULL,
    timestamp timestamp NOT NULL,
    id_oras   serial    NOT NULL,

    UNIQUE (id_oras, timestamp),
    CONSTRAINT FK_id_oras FOREIGN KEY(id_oras)
	REFERENCES Orase(id) ON DELETE CASCADE ON UPDATE CASCADE
);
