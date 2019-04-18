DROP TABLE instruments IF EXISTS;

CREATE TABLE instruments  (
    name VARCHAR(5) NOT NULL PRIMARY KEY,
    current_price DOUBLE,
    persisted_price DOUBLE,
    highest_price DOUBLE,
    second_highest_price DOUBLE,
    average_price DOUBLE
);

INSERT INTO instruments (
    name, 
    current_price, 
    persisted_price,
    highest_price, 
    second_highest_price, 
    average_price
) VALUES (
    'BT.L', 
    0.0,
    0.0,
    0.0,
    0.0,
    0.0
); 

INSERT INTO instruments (
    name, 
    current_price, 
    persisted_price,
    highest_price, 
    second_highest_price, 
    average_price
) VALUES (
    'VOD.L', 
    0.0,
    0.0,
    0.0,
    0.0,
    0.0
); 

INSERT INTO instruments (
    name, 
    current_price, 
    persisted_price,
    highest_price, 
    second_highest_price, 
    average_price
) VALUES (
    'BP.L', 
    0.0,
    0.0,
    0.0,
    0.0,
    0.0
); 

INSERT INTO instruments (
    name, 
    current_price, 
    persisted_price,
    highest_price, 
    second_highest_price, 
    average_price
) VALUES (
    'GOOG', 
    0.0,
    0.0,
    0.0,
    0.0,
    0.0
); 
