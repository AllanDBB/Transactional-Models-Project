// Script de inicialización para Neo4j
// Este archivo contiene queries Cypher para crear nodos y relaciones

// Crear constraints únicos
CREATE CONSTRAINT user_email IF NOT EXISTS FOR (u:User) REQUIRE u.email IS UNIQUE;
CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT order_id IF NOT EXISTS FOR (o:Order) REQUIRE o.id IS UNIQUE;

// Crear índices
CREATE INDEX user_username IF NOT EXISTS FOR (u:User) ON (u.username);
CREATE INDEX product_category IF NOT EXISTS FOR (p:Product) ON (p.category);
CREATE INDEX order_status IF NOT EXISTS FOR (o:Order) ON (o.status);

// Crear nodos de usuarios
CREATE (u1:User {
    id: 1,
    username: 'admin',
    email: 'admin@example.com',
    role: 'administrator',
    created_at: datetime(),
    active: true
})

CREATE (u2:User {
    id: 2,
    username: 'customer1',
    email: 'customer1@example.com',
    role: 'customer',
    created_at: datetime(),
    active: true
})

CREATE (u3:User {
    id: 3,
    username: 'supplier1',
    email: 'supplier1@example.com',
    role: 'supplier',
    created_at: datetime(),
    active: true
});

// Crear nodos de productos
CREATE (p1:Product {
    id: 1,
    name: 'Laptop Dell XPS 15',
    description: 'High-performance laptop',
    category: 'Electronics',
    price: 1299.99,
    stock: 45,
    created_at: datetime(),
    active: true
})

CREATE (p2:Product {
    id: 2,
    name: 'Python Programming Book',
    description: 'Learn Python from scratch',
    category: 'Books',
    price: 49.99,
    stock: 120,
    created_at: datetime(),
    active: true
})

CREATE (p3:Product {
    id: 3,
    name: 'Wireless Mouse',
    description: 'Ergonomic wireless mouse',
    category: 'Electronics',
    price: 29.99,
    stock: 200,
    created_at: datetime(),
    active: true
});

// Crear categorías
CREATE (c1:Category {name: 'Electronics', description: 'Electronic devices'})
CREATE (c2:Category {name: 'Books', description: 'Books and literature'});

// Crear relaciones
MATCH (p:Product {id: 1}), (c:Category {name: 'Electronics'})
CREATE (p)-[:BELONGS_TO]->(c);

MATCH (p:Product {id: 2}), (c:Category {name: 'Books'})
CREATE (p)-[:BELONGS_TO]->(c);

MATCH (p:Product {id: 3}), (c:Category {name: 'Electronics'})
CREATE (p)-[:BELONGS_TO]->(c);

MATCH (p:Product), (u:User {role: 'supplier'})
CREATE (u)-[:SUPPLIES]->(p);

// Crear orden de ejemplo
CREATE (o1:Order {
    id: 1,
    total_amount: 1329.98,
    status: 'delivered',
    payment_method: 'credit_card',
    created_at: datetime()
})

MATCH (u:User {id: 2}), (o:Order {id: 1})
CREATE (u)-[:PLACED]->(o);

MATCH (o:Order {id: 1}), (p:Product {id: 1})
CREATE (o)-[:CONTAINS {quantity: 1, unit_price: 1299.99}]->(p);

MATCH (o:Order {id: 1}), (p:Product {id: 3})
CREATE (o)-[:CONTAINS {quantity: 1, unit_price: 29.99}]->(p);

RETURN 'Neo4j initialization completed successfully!' as message;
