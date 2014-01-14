CREATE TABLE IF NOT EXISTS person
(
        id bigint primary key PRIMARY KEY AUTO_INCREMENT,
        first_name varchar(120) not null,
        last_name varchar(120) not null,
        birth_date date,
        deleted varchar(5) default 'false' not null
)

