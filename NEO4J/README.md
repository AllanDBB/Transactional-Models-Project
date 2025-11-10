# Neo4j - Transactional Models Project

## 🚀 Inicio Rápido

```bash
cd NEO4J
docker-compose up -d
```

## 📦 Servicios

- **Neo4j Browser**: http://localhost:7474
- **Bolt**: localhost:7687
- **Credenciales**: neo4j / password123

## 🔄 ETL Process

```bash
pip install -r etl/requirements.txt
python etl/run_etl.py
```

## 📁 Estructura

```
NEO4J/
├── docker-compose.yml
├── Dockerfile
├── init/
├── data/
└── etl/
```

## 📊 Queries Útiles

```cypher
// Ver todos los nodos
MATCH (n) RETURN n LIMIT 25;

// Ver usuarios y sus pedidos
MATCH (u:User)-[:PLACED]->(o:Order)
RETURN u, o;

// Ver productos por categoría
MATCH (p:Product)-[:BELONGS_TO]->(c:Category)
RETURN c.name, COUNT(p) as products;
```
