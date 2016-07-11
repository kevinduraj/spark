CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'Analytics' : 1};
CREATE TABLE IF NOT EXISTS test.cars (
  id text PRIMARY KEY,
  model text, 
  color text
);
CREATE INDEX ON test.cars (color);
INSERT INTO test.cars (id, model, color) VALUES ('KF-334L', 'Ford Mondeo', 'black');
INSERT INTO test.cars (id, model, color) VALUES ('MT-8787', 'Hyundai x35', 'black');
INSERT INTO test.cars (id, model, color) VALUES ('WX-2234', 'Toyota Yaris', 'silver');
INSERT INTO test.cars (id, model, color) VALUES ('AA-1234', 'Acura MDX', 'brown');
INSERT INTO test.cars (id, model, color) VALUES ('AS-8888', 'Aston Martin DB9 Volante', 'black');
INSERT INTO test.cars (id, model, color) VALUES ('AU-8930', 'Audi A3', 'blue');
INSERT INTO test.cars (id, model, color) VALUES ('BT-3920', 'Bentley Continental GT', 'black');
INSERT INTO test.cars (id, model, color) VALUES ('BM-2189', 'BMW 1', 'white');
INSERT INTO test.cars (id, model, color) VALUES ('BM-3808', 'BMW 5', 'red');
INSERT INTO test.cars (id, model, color) VALUES ('BU-3890', 'Buick Enclave', 'yellow');
INSERT INTO test.cars (id, model, color) VALUES ('BU-9302', 'Buick Lacrosse', 'brown');
INSERT INTO test.cars (id, model, color) VALUES ('CA-9098', 'Cadillac CTS Coupe', 'beige');
INSERT INTO test.cars (id, model, color) VALUES ('CV-9872', 'Chevrolet Camaro Coupe', 'cyan');
INSERT INTO test.cars (id, model, color) VALUES ('CV-0200', 'Chevrolet Corvette', 'orange');
INSERT INTO test.cars (id, model, color) VALUES ('CR-9244', 'Chrysler 300', 'brown');
INSERT INTO test.cars (id, model, color) VALUES ('DG-2222', 'Dodge Avenger', 'black');
INSERT INTO test.cars (id, model, color) VALUES ('DG-8897', 'Dodge Charger', 'black');
INSERT INTO test.cars (id, model, color) VALUES ('FR-8877', 'Ferrari FF', 'silver');
INSERT INTO test.cars (id, model, color) VALUES ('FI-7777', 'Fiat 500', 'blue');
INSERT INTO test.cars (id, model, color) VALUES ('FD-9399', 'Ford E-150 Van', 'white');
INSERT INTO test.cars (id, model, color) VALUES ('FD-2003', 'Ford Edge', 'gray');
INSERT INTO test.cars (id, model, color) VALUES ('FD-2283', 'Ford Explorer', 'green');
INSERT INTO test.cars (id, model, color) VALUES ('GM-1229', 'GMC Canyon', 'purple');
INSERT INTO test.cars (id, model, color) VALUES ('HD-1828', 'Honda Accord', 'silver');
INSERT INTO test.cars (id, model, color) VALUES ('IN-9964', 'Infinity FX', 'black');
INSERT INTO test.cars (id, model, color) VALUES ('JG-0033', 'Jaguar XF', 'green');
INSERT INTO test.cars (id, model, color) VALUES ('JE-9350', 'Jeep Compass', 'brown');
INSERT INTO test.cars (id, model, color) VALUES ('KI-5557', 'Kia Optima', 'white');
INSERT INTO test.cars (id, model, color) VALUES ('LA-0887', 'Lamborghini Gallardo Spyder', 'red');
INSERT INTO test.cars (id, model, color) VALUES ('LX-5773', 'Lexus ES', 'turquoise');
INSERT INTO test.cars (id, model, color) VALUES ('LN-4448', 'Lincoln Navigator', 'beige');
INSERT INTO test.cars (id, model, color) VALUES ('MZ-1038', 'Mazda CX-9', 'black');
