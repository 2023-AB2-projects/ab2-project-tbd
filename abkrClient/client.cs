using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace abkr.Client
{
    class Client
    {
        static void Main(string[] args)
        {
            // connect to server
            TcpClient client = new TcpClient();
            client.Connect(IPAddress.Parse("127.0.0.1"), 1234);

            Console.WriteLine("Connected to server. Enter SQL statements or type 'exit' to quit:");

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

                // send SQL statement to server
                byte[] data = Encoding.ASCII.GetBytes(sqlStatement + "\n");

                NetworkStream stream = client.GetStream();
                stream.Write(data, 0, data.Length);

                // receive response from server
                data = new byte[1024];
                int bytesRead = stream.Read(data, 0, data.Length);
                string response = Encoding.ASCII.GetString(data, 0, bytesRead);
                Console.WriteLine(response);
            }

            // close connection
            client.Close();
            Console.WriteLine("Disconnected from server.");
        }
    }
}
