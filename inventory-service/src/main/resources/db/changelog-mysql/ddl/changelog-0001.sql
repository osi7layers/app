CREATE TABLE IF NOT EXISTS inventory
(
    id            BIGINT NOT NULL AUTO_INCREMENT,
    `productName` VARCHAR(255) NULL,
    quantity      INT NULL,
    PRIMARY KEY (id)
);
