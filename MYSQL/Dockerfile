FROM mysql:8.0

# Instalar herramientas adicionales
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Crear directorio para scripts
RUN mkdir -p /docker-entrypoint-initdb.d

# Copiar scripts de inicialización
COPY init/*.sql /docker-entrypoint-initdb.d/

# Configurar timezone
ENV TZ=America/Costa_Rica
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Exponer puerto
EXPOSE 3306

# El comando por defecto ya está definido en la imagen base
