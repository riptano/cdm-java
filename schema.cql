create table users ( name text primary key, num_tacos int );

CREATE TABLE alltypes (
    id uuid PRIMARY KEY,
    avg float,
    cash decimal,
    intmap map<int, float>,
    num int,
    ts timeuuid
);
