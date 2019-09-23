# --- !Ups

CREATE TABLE Post (
    id bigint(20) NOT NULL AUTO_INCREMENT,
    country varchar(255) NOT NULL,
    motor_code varchar(255) NOT NULL,
    motor varchar(255)NOT NULL,
    power varchar(255)NOT NULL,
    series varchar(255)NOT NULL,
    series_type varchar(255)NOT NULL,
    Fuel varchar(255)NOT NULL,
    transmission varchar(255)NOT NULL,
    transmission_type varchar(255)NOT NULL,
    displacement varchar(255)NOT NULL,
    drive varchar(255)NOT NULL,
    hybrid varchar(255)NOT NULL,
    mileage varchar(255)NOT NULL,
    carage varchar(255)NOT NULL,
    motorstarts varchar(255)NOT NULL,
    KIEFA varchar(255)NOT NULL,
    defect_02 varchar(255)NOT NULL,
    defect_04 varchar(255)NOT NULL,
    PRIMARY KEY (id)
);

# --- !Downs
DROP TABLE Post;