FROM apache/hive:4.0.1

# La imagen oficial de Apache Hive NO incluye el driver JDBC de PostgreSQL,
# que el metastore necesita para conectarse a la base de datos PostgreSQL.
# Sin este jar, el metastore falla con:
#   java.lang.ClassNotFoundException: org.postgresql.Driver
# y entra en un bucle de reinicio.
USER root
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/hive/lib/postgresql-42.7.4.jar
RUN chmod 644 /opt/hive/lib/postgresql-42.7.4.jar

# Warehouse compartido entre contenedores (Hive y Spark/Jupyter).
# Permisos 1777 (como /tmp) para que escriban tanto el usuario 'hive' como
# 'jovyan', que tienen UID distinto. Esta ruta se monta como volumen comun.
RUN mkdir -p /warehouse && chmod 1777 /warehouse
USER hive
