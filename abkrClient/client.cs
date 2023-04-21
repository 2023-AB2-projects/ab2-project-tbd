using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace abkr.Client
{
    class Client
    {
        static async Task Main(string[] args)
        {
            // Connect to server
            TcpClient client = new TcpClient();
            await client.ConnectAsync(IPAddress.Parse("127.0.0.1"), 1234);

            Console.WriteLine("Connected to server. Enter SQL statements or type 'exit' to quit:");

            using NetworkStream stream = client.GetStream();
            using StreamReader reader = new StreamReader(stream, Encoding.ASCII);
            using StreamWriter writer = new StreamWriter(stream, Encoding.ASCII) { AutoFlush = true };

            while (true)
            {
                // Read SQL statement from the command line
                Console.Write("> ");
                string sqlStatement = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(sqlStatement))
                {
                    continue;
                }

                if (sqlStatement.ToLower() == "exit")
                {
                    break;
                }

                // Send SQL statement to server
                await writer.WriteLineAsync(sqlStatement);

                // Receive response from server
                string response = await reader.ReadLineAsync();
                Console.WriteLine(response);
            }

            // Close connection
            client.Close();
            Console.WriteLine("Disconnected from server.");
        }
    }
}
