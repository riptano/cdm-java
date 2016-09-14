create table users ( name text primary key, num_tacos int );

CREATE TABLE alltypes (
    id uuid PRIMARY KEY,
    avg float,
    cash decimal,
    intmap map<int, float>,
    num int,
    ts timeuuid
);

create table maptest (
    id int,
    m map<text, text>,
    primary key (id)
);

// Users keyed by id
CREATE TABLE users2 (
    userid uuid,
    firstname text,
    lastname text,
    email text,
    created_date timestamp,
    PRIMARY KEY (userid)
);
