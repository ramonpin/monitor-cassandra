Monitor Cassandra
=================
Un pequeño script para recoger varias métricas del estado de los nodos 
de un sistema Cassandra mientras este se está ejecutando.

El objetivo es que el script no requiera la instalación de procesos 
especiales en los nodos de Cassandra. Solo debe ser necesario el acceso
a los mismos mediante ssh (usuario/password).

Los datos de conexión se configuran en el fichero 'connection.yml'. Para
ello copia o renombra el 'connection.yml.template' y editalo para añadir
los nodos, puerto, usuario y password.
