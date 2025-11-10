FROM neo4j:5.15

# Configurar variables de entorno
ENV NEO4J_AUTH=neo4j/password123
ENV NEO4J_PLUGINS='["apoc", "graph-data-science"]'
ENV NEO4J_dbms_memory_pagecache_size=512M
ENV NEO4J_dbms_memory_heap_max__size=1G

# Crear directorio para scripts
RUN mkdir -p /var/lib/neo4j/import

# Copiar scripts de inicialización
COPY init/*.cypher /var/lib/neo4j/import/

# Exponer puertos
EXPOSE 7474 7687

# El comando por defecto ya está definido en la imagen base
