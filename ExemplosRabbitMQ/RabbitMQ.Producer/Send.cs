using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMQ.Producer {
    class Send {
        static void Main(string[] args) {

            // EnvioSimplesParaFila();
            // EnvioWork(args);
            // EnvioWorkAck(args);
            // EnvioComExchange_Fanout(args);
            // EnvioComExchange_Direct(args);
            EnvioComExchange_Topic(args);

            // TODO: A fazer!
            // Fazer o exchange enviar para a fila, para se não tiver consumer, ele pega da fila depois.
            // Estudar e fazer o exemplo de RPC:
            // - https://www.rabbitmq.com/tutorials/tutorial-six-dotnet.html
            // Enviar classe para a lista
        }

        // https://www.rabbitmq.com/tutorials/tutorial-one-dotnet.html
        private static void EnvioSimplesParaFila() {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {
                    channel.QueueDeclare(queue: "hello",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    // Mensagem a ser enviada
                    var totalMensagens = 0;
                    for (totalMensagens = 0; totalMensagens < 5; totalMensagens++) {
                        // Prepara a mensagem
                        string message = string.Format("Mensagem enviada pelo producer para a fila [Hello]: {0}", totalMensagens);
                        var body = Encoding.UTF8.GetBytes(message);

                        // Publica na fila especificada no (channel.QueueDeclare(queue: <<nomeDaFile>>))
                        channel.BasicPublish(exchange: "",
                            routingKey: "hello",
                            basicProperties: null,
                            body: body);
                    }

                    Console.WriteLine("[x] Total de mensagens enviadas: {0}", totalMensagens);
                }
            }

            Console.WriteLine("Press [enter] to exit.");
            Console.ReadLine();
        }

        // NewTask
        // Para usar o Envio Work, abra vários consumers e chame o producer pelo cmd
        // Chame usando pontos finais..
        // Exemplo: 
        // RabbitMQ.Producer.exe First message.
        // RabbitMQ.Producer.exe second message..
        // RabbitMQ.Producer.exe third message...
        // Pois cada ponto, faz ele dar um sleep antes de enviar para o consumer
        // E cada consumer que estiver levantado, pega da fila na ordem que foram levantados.
        // https://www.rabbitmq.com/tutorials/tutorial-two-dotnet.html
        private static void EnvioWork(string[] args) {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {
                    channel.QueueDeclare(queue: "task_queue",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    // Recebe a mensagem por parâmetro
                    string message = GetMessage(args);
                    var body = Encoding.UTF8.GetBytes(message);

                    // Marca como uma mensagem persistente
                    var properties = channel.CreateBasicProperties();
                    var delivery = properties.Persistent = true;

                    // Publica na fila especificada no (channel.QueueDeclare(queue: <<nomeDaFile>>))
                    channel.BasicPublish(exchange: "",
                        routingKey: "task_queue",
                        basicProperties: properties,
                        body: body);

                    Console.WriteLine("[x] Total de mensagens enviadas: {0}");
                }
            }

            Console.WriteLine("Press [enter] to exit.");
            Console.ReadLine();
        }

        private static void EnvioWorkAck(string[] args) {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {
                    channel.QueueDeclare(queue: "task_queue_ack",
                        durable: true,                 // Quando a mensagem é durável, ela não se perde, mesmo se o RabbitMQ server cair
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    // =========================================================> Fair dispatch <=
                    // Resolve o problema de quando um worker recebe muitas mensagem pesadas
                    // E o outro recebe muitas mensagens leves. Um vai estar constantemente ocupado
                    // Enquanto o outro raramente faz algum trabalho.
                    // =========================================================>
                    // In order to defeat that we can use the basicQos method with the prefetchCount = 1 setting.
                    // This tells RabbitMQ not to give more than one message to a worker at a time. 
                    // Or, in other words, don't dispatch a new message to a worker until it has 
                    // processed and acknowledged the previous one. Instead, it will dispatch it 
                    // to the next worker that is not still busy.
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 2, global: false);

                    // Recebe a mensagem por parâmetro
                    string message = GetMessage(args);
                    var body = Encoding.UTF8.GetBytes(message);

                    // Marca como uma mensagem persistente
                    var properties = channel.CreateBasicProperties();
                    var delivery = properties.Persistent = true;

                    // Publica na fila especificada no (channel.QueueDeclare(queue: <<nomeDaFile>>))
                    channel.BasicPublish(exchange: "",
                        routingKey: "task_queue_ack",
                        basicProperties: properties,
                        body: body);

                    Console.WriteLine("[x] Total de mensagens enviadas: {0}");
                }
            }

            Console.WriteLine("Press [enter] to exit.");
            Console.ReadLine();
        }

        private static void EnvioComExchange_Fanout(string[] args) {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {

                    // Prepara a mensagem
                    var message = GetMessage(args);
                    var body = Encoding.UTF8.GetBytes(message);

                    // Publica na fila especificada no (channel.QueueDeclare(queue: <<nomeDaFile>>))
                    // Usando Exchange
                    // - https://www.rabbitmq.com/tutorials/tutorial-three-dotnet.html
                    // Cria um Exchange do tipo fanout
                    // Explicação dos tipos de exchange
                    // - https://www.rabbitmq.com/tutorials/amqp-concepts.html
                    channel.ExchangeDeclare(exchange: "logs", type: "fanout");
                    channel.BasicPublish(exchange: "logs",  // Nome do exchange
                        routingKey: "",
                        basicProperties: null,
                        body: body);

                    Console.WriteLine("[x] Enviado {0}", message);
                }
            }

            Console.WriteLine("Press [enter] to exit.");
            Console.ReadLine();
        }

        // Enviando msg direta com filtro:
        // RabbitMQ.Producer.exe error "Mensagem de ERROR!"
        // RabbitMQ.Producer.exe info "Mensagem de INFO!"
        // RabbitMQ.Producer.exe warning "Mensagem de WARNING!"
        private static void EnvioComExchange_Direct(string[] args) {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {

                    // Prepara a mensagem
                    var severity = (args.Length > 0) ? args[0] : "info";
                    var message = (args.Length > 1)
                                  ? string.Join(" ", args.Skip(1).ToArray())
                                  : "Hello World!";

                    var body = Encoding.UTF8.GetBytes(message);

                    // Publica na fila especificada no (channel.QueueDeclare(queue: <<nomeDaFile>>))
                    // Usando Exchange
                    // - https://www.rabbitmq.com/tutorials/tutorial-three-dotnet.html
                    // Cria um Exchange do tipo fanout
                    // Explicação dos tipos de exchange
                    // - https://www.rabbitmq.com/tutorials/amqp-concepts.html
                    channel.ExchangeDeclare(exchange: "direct_logs", type: "direct");
                    channel.BasicPublish(exchange: "direct_logs",  // Nome do exchange
                        routingKey: severity,
                        basicProperties: null,
                        body: body);

                    Console.WriteLine("[x] Enviado '{0}':'{1}'", severity, message);
                }
            }

            Console.WriteLine("Press [enter] to exit.");
            Console.ReadLine();
        }

        // Enviando log com o tipo de routing key "kern.critical" 
        // RabbitMQ.Producer.exe "kern.critical" "A critical kernel error"
        // https://www.rabbitmq.com/tutorials/tutorial-five-dotnet.html
        private static void EnvioComExchange_Topic(string[] args) {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {

                    channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");

                    // Prepara a mensagem
                    var routingKey = (args.Length > 0) ? args[0] : "anonymous.info";
                    var message = (args.Length > 1)
                                  ? string.Join(" ", args.Skip(1).ToArray())
                                  : "Hello World!";

                    var body = Encoding.UTF8.GetBytes(message);

                    // Publica na fila especificada no (channel.QueueDeclare(queue: <<nomeDaFile>>))
                    // Usando Exchange
                    // - https://www.rabbitmq.com/tutorials/tutorial-three-dotnet.html
                    // Cria um Exchange do tipo fanout
                    // Explicação dos tipos de exchange
                    // - https://www.rabbitmq.com/tutorials/amqp-concepts.html

                    channel.BasicPublish(exchange: "topic_logs",  // Nome do exchange
                        routingKey: routingKey,
                        basicProperties: null,
                        body: body);

                    Console.WriteLine("[x] Enviado '{0}':'{1}'", routingKey, message);
                }
            }

            Console.WriteLine("Press [enter] to exit.");
            Console.ReadLine();
        }

        private static string GetMessage(string[] args) {
            return ((args.Length > 0) ? string.Join(" ", args) : "info: Hello World!");
        }
    }
}
