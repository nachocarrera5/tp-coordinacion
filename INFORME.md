# TP Coordinacion

## Descripcion general

El sistema procesa consultas de multiples clientes de manera concurrente. El gateway asigna un uuid a cada uno el cual se incluye en todos los mensajes internos, esto logra que en cada etapa se pueda mantener por separado el estado de las consultas.

Los registros se distribuyen entre las replicas de Sum usando una cola compartida, cada replica calcula los subtotales, y estos despues se dividen entre los Aggregations. Cada uno de estos determina un top parcial, que se lo envia al Join quien combina los resultados para producir el top final. Por ultimo el gateway le entrega ese top final al cliente que corresponda.

## Procesamiento de multiples clientes

Cada conexion que recibe el Gateway tiene asociado un MessageHandler que genera un UUID para identificar al cliente. Como mencione previamente, ese id se agrega en todos los mensajes internos, tanto los registros de frutas como los mensajes de finalizacion y resultado.

Aunque los mensajes de varios clientes lleguen intercalados a las mismas colas, los contenidos no se mezclan porque las distintas etapas usan el identificador como clave para almacenar el estado de la consulta. Y despues ya con el resultado logicamente el Gateway lo usa para encontrar en donde enviar el resultado.

## Coordinacion de replicas de Sum

Las instancias de Sum consumen los registros desde una misma cola. Rabbit entrega cada mensaje a una sola de ellas y el trabajo se distribuye entre las replicas y cada una calcula los subtotales de los registros que recibe.

Adicional a la configuracion del TP MOM, se configura un prefetch de un mensaje para evitar que una instancia reserve muchos mensajes sin procesarlos.

El EOF que envia el Gateway llega inicialmente a una sola instancia de Sum, la cual lo publica en un exchange de control, entonces cada Sum recibe la notificacion de finalizacion y envia los subtotales de ese cliente.

Cada Sum consume datos y mensajes de control en diferentes hilos. Para que no que accedan al estado de un mismo cliente en simultaneo, las operaciones de ese estado se protegen con un lock.

## Distribucion entre Aggregation

Cada subtotal que produce una instancia de Sum se envia a un solo Aggregator. Para elegirlo se usa un hash que se construye a partir del id del cliente y del propio nombre de la fruta, y se obtiene el resto de dividir por la cantidad de Aggregations. Todas las instancias aplican la misma funcion entonces los subtotales de una misma fruta y un mismo cliente llegan al mismo destino.

Asi se evita mandar todos los datos a todas las replicas y permite que las instancias de Aggregation usen conjuntos distintos.

Despues de enviar los subtotales, cada Sum envia un EOF a cada Aggregation, y cada uno de estos tiene un contador por cliente y espera a recibir un EOF por cada instancia de Sum para saber que termino y puede calcular su top parcial.

## Construccion del top final

Cuando ya un Aggregation recibio los EOF de todas las instancias de Sum, ordena las frutas de su particion y conserva solamente la cantidad indiciada por TOP_SIZE. Asi se genera un top parcial sin enviar al Join todas las frutas.

El Join guarda esos registros y tiene para cada cliente un contador de resultados parciales.

Cuando recibe un resultado de todas las instancias de Aggregation, ordena los candidatos y conserva los primeros TOP_SIZE y envia el top final al Gateway.

Con que cada Aggregation envie como maximo TOP_SIZE frutas alcanza, porque si una queda fuera del top de su propia particion, entonces en esa particion ya existen al menos TOP_SIZE frutas que la superan segun el criterio de comparacion definido por FruitItem. Por lo tanto, tampoco podria formar parte del top final.

## Escalabilidad y recursos

El Gateway puede atender multiples conexiones de manera concurrente y asignar un id diferente a cada cliente. Las siguientes etapas comparten colas y replicas pero el estado lo mantienen separado con ese id. No es necesario crear un conjunto de controles o colas nuevo para cada consulta.

El archivo no se carga entero de entrada en memoria. Las instancias de Sum procesan a medida que llegan los mensajes y solo conservan un subtotal por fruta y cliente. Cuando terminan es eso lo que mandan, en vez de reenviar todos los registros originales.

En Aggregation vuelve a reducirse la cantidad de datos ya que en cada instancia envia al Join como maximo TOP_SIZE elementos.

Los exchange que se usan solo para publicar no crean colas propios para no almacenar copias de mensajes que nunca se van a consumir.

Si agregamos replicas de Sum, compiten por los mensajes de la cola compartida y se distribuyen el procesamiento de los registros, entonces el exchange de control permite que todas conozcan cuando termina cada cliente, habiendo o no recibido el EOF original.

Si agregamos replicas de Aggregation, el hash distribuye las combinaciones de cliente y fruta entre ellas. Cada una procesa una particion difierente y el contador del Join depende de la cantidad configurada de Aggregations para poder esperar un resultado parcial de cada una de ellas.

## Cierre ordenado

Las instancias de Sum, Aggregation y Join tienen un handler para la señal SIGTERM. Cuando reciben la señal le piden al middleware que detenga el consumo de mensajes, permitiendo que el flujo normal del programa llegue a los bloques en donde se cierran las conexiones con RabbitMQ.

El middleware usa add_callback_threadsafe para solicitar que pare el consumo de forma segura, incluso si la conexion esta siendo utilizada desde otro hilo.

Sum requiere un tratamiento adicional porque consume la cola de datos y el exchange de control en hilos diferentes. Cuando termina el hilo principal se detiene el consumidor de control, se espera que termine mediante join y luego se cierran las conexiones restantes. Esto evita finalizar el proceso mientras todavia hay un hilo utilizando recursos del middleware.