using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQ.Consumer {

    internal class Receive {

        private static void Main(string[] args) {
            // ConsumoSimplesDaFila();
            // ConsumoWork();
            // ConsumoWorkAck();
            // ConsumoEnvioComExchange_Fanout();
            // ConsumoEnvioComExchange_Direct(args);
            // ConsumoEnvioComExchange_Topic(args);
            ConsumoRPC();
        }

        // https://www.rabbitmq.com/tutorials/tutorial-one-dotnet.html
        private static void ConsumoSimplesDaFila() {
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

                    // Cria um consumer básico
                    var consumer = new EventingBasicConsumer(channel);

                    // Cria um evento que será disparado quando tiver algum item para ser recebidos
                    consumer.Received += (model, routingkey) => {
                        var body = routingkey.Body; // Pega a mensagem pela rota
                        var message = Encoding.UTF8.GetString(body); // Converte os bytes em string

                        Console.WriteLine("[x] Mensagem recebida: {0}", message);
                    };

                    channel.BasicConsume(queue: "hello",
                        noAck: true,
                        consumer: consumer);

                    Console.WriteLine("Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }

        // Consumer = work
        // https://www.rabbitmq.com/tutorials/tutorial-two-dotnet.html
        private static void ConsumoWork() {
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

                    // Cria um consumer básico
                    var consumer = new EventingBasicConsumer(channel);

                    // Cria um evento que será disparado quando tiver algum item para ser recebidos
                    consumer.Received += (model, routingkey) => {
                        var body = routingkey.Body; // Pega a mensagem pela rota
                        var message = Encoding.UTF8.GetString(body); // Converte os bytes em string

                        int dots = message.Split('.').Length - 1;
                        Thread.Sleep(dots * 1000);

                        Console.WriteLine("[x] Mensagem recebida: {0}", message);
                    };

                    channel.BasicConsume(queue: "task_queue",
                        noAck: true,
                        consumer: consumer);

                    Console.WriteLine("Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }

        // Note on message persistence
        // Marking messages as persistent doesn't fully guarantee that a message won't be lost.Although
        // it tells RabbitMQ to save the message to disk, there is still a short time window when
        // RabbitMQ has accepted a message and hasn't saved it yet. Also, RabbitMQ doesn't do fsync(2)
        // for every message -- it may be just saved to cache and not really written to the disk.
        // The persistence guarantees aren't strong, but it's more than enough for our simple task queue.
        // If you need a stronger guarantee then you can use (publisher confirms)[1].
        // [1] - https://www.rabbitmq.com/confirms.html
        private static void ConsumoWorkAck() {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {
                    channel.QueueDeclare(queue: "task_queue_ack",
                        durable: true, // Quando a mensagem é durável, ela não se perde, mesmo se o RabbitMQ server cair
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    // Cria um consumer básico
                    var consumer = new EventingBasicConsumer(channel);

                    // Cria um evento que será disparado quando tiver algum item para ser recebidos
                    consumer.Received += (model, routingkey) => {
                        var body = routingkey.Body; // Pega a mensagem pela rota
                        var message = Encoding.UTF8.GetString(body); // Converte os bytes em string

                        Console.WriteLine("[x] Mensagem recebida: {0}", message);

                        int dots = message.Split('.').Length - 1;
                        Thread.Sleep(dots * 1000);

                        Console.WriteLine("[x] Fim da mensagem.");

                        // Usando esse código, asseguramos que mesmo se um worker cair
                        // enquanto a mensagem é processada, nenhuma informação será perdida.
                        channel.BasicAck(deliveryTag:
                            routingkey.DeliveryTag,
                            multiple: false);
                    };

                    channel.BasicConsume(queue: "task_queue_ack",
                        noAck: false, // Habilita o ack(nowledgments)
                        consumer: consumer);

                    Console.WriteLine("Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }

        // Quando usamos fanout, ele ignora qualquer fila
        private static void ConsumoEnvioComExchange_Fanout() {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {
                    channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                    // Quando criamos uma fila sem parâmentros usando QueueDeclare()
                    // É criada uma fila não durável, exclusiva e autoDelete com um nome gerado automáticamente
                    // Ex:
                    // amq.gen-JzTY20BRgKO-HjmUJj0wLg
                    var queueName = channel.QueueDeclare().QueueName;
                    channel.QueueBind(queue: queueName,
                        exchange: "logs",
                        routingKey: "");

                    Console.WriteLine("[*] Aguardando por logs..");

                    // Cria um consumer básico
                    var consumer = new EventingBasicConsumer(channel);

                    // Cria um evento que será disparado quando tiver algum item para ser recebidos
                    consumer.Received += (model, routingkey) => {
                        var body = routingkey.Body; // Pega a mensagem pela rota
                        var message = Encoding.UTF8.GetString(body); // Converte os bytes em string

                        Console.WriteLine("[x] Mensagem recebida: {0}", message);
                    };

                    channel.BasicConsume(queue: queueName,
                        noAck: true,
                        consumer: consumer);

                    Console.WriteLine("Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }

        // Usando filtrando por error:
        // RabbitMQ.Consumer.exe error
        private static void ConsumoEnvioComExchange_Direct(string[] args) {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {
                    channel.ExchangeDeclare(exchange: "direct_logs", type: "direct");

                    // Quando criamos uma fila sem parâmentros usando QueueDeclare()
                    // É criada uma fila não durável, exclusiva e autoDelete com um nome gerado automáticamente
                    // Ex:
                    // amq.gen-JzTY20BRgKO-HjmUJj0wLg
                    var queueName = channel.QueueDeclare().QueueName;

                    if (args.Length < 1) {
                        Console.Error.WriteLine("Usage: {0} [info] [warning] [error]",
                            Environment.GetCommandLineArgs()[0]);
                        Console.WriteLine("Press [enter] to exit.");
                        Console.ReadLine();
                        Environment.ExitCode = 1;
                        return;
                    }

                    foreach (var severity in args) {
                        channel.QueueBind(queue: queueName,
                            exchange: "direct_logs",
                            routingKey: severity);
                    }

                    Console.WriteLine("[*] Aguardando por mensagens..");

                    // Cria um consumer básico
                    var consumer = new EventingBasicConsumer(channel);

                    // Cria um evento que será disparado quando tiver algum item para ser recebidos
                    consumer.Received += (model, routingkey) => {
                        var body = routingkey.Body; // Pega a mensagem pela rota
                        var message = Encoding.UTF8.GetString(body); // Converte os bytes em string

                        Console.WriteLine("[x] Mensagem recebida: {0}", message);
                    };

                    channel.BasicConsume(queue: queueName,
                        noAck: true,
                        consumer: consumer);

                    Console.WriteLine("Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }

        // Recebendo todos os logs:
        // RabbitMQ.Consumer.exe #
        // Recebendo logs do "kern":
        // RabbitMQ.Consumer.exe "kern.*"
        // Ouvindo apenas "critical"
        // RabbitMQ.Consumer.exe "*.critical"
        // Usando múltiplos bindings:
        // RabbitMQ.Consumer.exe "kern.*" "*.critical"
        // https://www.rabbitmq.com/tutorials/tutorial-five-dotnet.html
        private static void ConsumoEnvioComExchange_Topic(string[] args) {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {
                    channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");

                    // Quando criamos uma fila sem parâmentros usando QueueDeclare()
                    // É criada uma fila não durável, exclusiva e autoDelete com um nome gerado automáticamente
                    // Ex:
                    // amq.gen-JzTY20BRgKO-HjmUJj0wLg
                    var queueName = channel.QueueDeclare().QueueName;

                    if (args.Length < 1) {
                        Console.Error.WriteLine("Usage: {0} [binding_key...]", Environment.GetCommandLineArgs()[0]);
                        Console.WriteLine("Press [enter] to exit.");
                        Console.ReadLine();
                        Environment.ExitCode = 1;
                        return;
                    }

                    foreach (var bindingKey in args) {
                        channel.QueueBind(queue: queueName,
                            exchange: "direct_logs",
                            routingKey: bindingKey);
                    }

                    Console.WriteLine("[*] Aguardando por mensagens..");

                    // Cria um consumer básico
                    var consumer = new EventingBasicConsumer(channel);

                    // Cria um evento que será disparado quando tiver algum item para ser recebidos
                    consumer.Received += (model, routingkey) => {
                        var body = routingkey.Body; // Pega a mensagem pela rota
                        var message = Encoding.UTF8.GetString(body); // Converte os bytes em string

                        Console.WriteLine("[x] Mensagem recebida: {0}", message);
                    };

                    channel.BasicConsume(queue: queueName,
                        noAck: true,
                        consumer: consumer);

                    Console.WriteLine("Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }

        // O consumer procura na fila "rpc_queue" a mensagem com o número a ser resolvido
        // Após recuperar da fila, chama o método fibonacci e fica esperando a resposta
        // Quando o cálculo termina, ele envia a resposta para a fila
        // O producer pega a resposta da fila e mostra
        // https://www.rabbitmq.com/tutorials/tutorial-six-dotnet.html
        private static void ConsumoRPC() {
            // Instalando RabbitMQ no projeto
            // Install-Package RabbitMQ.Client

            // Host a se conectar
            var factory = new ConnectionFactory() { HostName = "localhost" };

            // Cria uma conexão específica para o endpoint
            using (var connection = factory.CreateConnection()) {
                // Abre um canal e cria a fila
                using (var channel = connection.CreateModel()) {
                    // Declara a fila
                    channel.QueueDeclare(queue: "rpc_queue",
                        durable: false,
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
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                    // Cria um consumer básico
                    var consumer = new QueueingBasicConsumer(channel);

                    // Inicia um consumer básico
                    channel.BasicConsume(queue: "rpc_queue",
                        noAck: false,
                        consumer: consumer);


                    Console.WriteLine("[x] Aguardando requisição RPC..");

                    // Loop que fica aguardando a mensagem
                    // Faz o work e envia a resposta de volta
                    while (true) {
                        string response = null;

                        // Recupera o primeiro item da fila..
                        var ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                        var body = ea.Body;
                        var props = ea.BasicProperties;
                        var replyPros = channel.CreateBasicProperties();
                        replyPros.CorrelationId = props.CorrelationId;


                        try {

                            // Recupera a mensagem
                            var message = Encoding.UTF8.GetString(body);

                            // O exemplo é com fibonnaci, por isso converte para inteiro
                            int n = int.Parse(message);

                            Console.WriteLine("[.] fibonacci({0})", message);

                            // Chama a função recursiva de fibonnaci
                            response = fibonacci(n).ToString();
                        } catch (Exception ex) {
                            Console.WriteLine(" [.] " + ex.Message);
                            response = "";
                        } finally {
                            var responseBytes = Encoding.UTF8.GetBytes(response);

                            // Publica na fila específicada
                            channel.BasicPublish(exchange: "",
                                routingKey: props.ReplyTo,
                                basicProperties: replyPros,
                                body: responseBytes);

                            // Configura o reconhecimento de mensagem
                            channel.BasicAck(deliveryTag: ea.DeliveryTag,
                                multiple: false);
                        }

                    }
                }
            }

        }

        /// <summary>
        /// Assumes only valid positive integer input.
        /// Don't expect this one to work for big numbers,
        /// and it's probably the slowest recursive implementation possible.
        /// </summary>
        private static int fibonacci(int n) {
            if (n == 0 || n == 1) {
                return n;
            }

            return fibonacci(n - 1) + fibonacci(n - 2);
        }
    }
}