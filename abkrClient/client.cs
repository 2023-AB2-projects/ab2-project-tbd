using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using abkr.ClientLogger;

namespace abkr.Client
{
    class Client
    {
        private static Logger logger = new Logger("C:/Users/bfcsa/github-classroom/2023-AB2-projects/ab2-project-tbd/abkrClient/client_logger.log");
        static async Task Main(string[] args)
        {
            bool isSelectStatement = false;

            // Connect to server
            TcpClient client = new TcpClient();
            await client.ConnectAsync(IPAddress.Parse("127.0.0.1"), 1234);

            logger.LogMessage("Connected to server. Enter SQL statements or type 'exit' to quit:");

            using NetworkStream stream = client.GetStream();
            using StreamReader reader = new StreamReader(stream, Encoding.ASCII);
            using StreamWriter writer = new StreamWriter(stream, Encoding.ASCII) { AutoFlush = true };

            while (true)
            {
                // Read SQL statement from the command line
                Console.Write("> ");
                string? sqlStatement = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(sqlStatement))
                {
                    continue;
                }

                if (sqlStatement.ToLower() == "exit")
                {
                    break;
                }

                if (sqlStatement.ToLower().StartsWith("select"))
                {
                    isSelectStatement = true;
                }


                // Send SQL statement to server
                await writer.WriteLineAsync(sqlStatement);

                // Receive response from server
                string? response = await reader.ReadLineAsync();
                logger.LogMessage($"{response}");

                //if (isSelectStatement)
                //{
                //    // Open a separate channel to receive data from the server

                //    // Create a new TcpClient and connect to the server
                //    TcpClient dataClient = new TcpClient();
                //    await dataClient.ConnectAsync(IPAddress.Parse("127.0.0.1"), 1235);

                //    using NetworkStream dataStream = dataClient.GetStream();
                //    using StreamReader dataReader = new StreamReader(dataStream, Encoding.ASCII);

                //    // Receive data from the server
                //    string responseData = await dataReader.ReadToEndAsync();
                //    Console.WriteLine(responseData);

                //    // Close the data channel
                //    dataClient.Close();

                //    // Reset the flag for the next SQL statement
                //    isSelectStatement = false;
                //}
            }

            // Close connection
            client.Close();
            logger.LogMessage("Disconnected from server.");
        }
    }
}
