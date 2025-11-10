FROM postgres:15-alpine

# Instalar extensiones y herramientas
RUN apk add --no-cache \
    curl \
    vim \
    && rm -rf /var/cache/apk/*

# Crear directorio para scripts
RUN mkdir -p /docker-entrypoint-initdb.d

# Copiar scripts de inicialización
COPY init/*.sql /docker-entrypoint-initdb.d/

# Configurar timezone
ENV TZ=America/Costa_Rica
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Exponer puerto
EXPOSE 5432
