using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace MiniSQL.Client;

class Program
{
    static void Main(string[] args)
    {
        // connect to server
        TcpClient client = new TcpClient();
        client.Connect(IPAddress.Parse("127.0.0.1"), 1234);
        // send SQL statement to server
        string sqlStatement = "CREATE TABLE myTable (id INT PRIMARY KEY, name VARCHAR(50))";
        byte[] data = Encoding.ASCII.GetBytes(sqlStatement);
        NetworkStream stream = client.GetStream();
        stream.Write(data, 0, data.Length);

        // receive response from server
        data = new byte[1024];
        int bytesRead = stream.Read(data, 0, data.Length);
        string response = Encoding.ASCII.GetString(data, 0, bytesRead);
        Console.WriteLine(response);

        // close connection
        stream.Close();
        client.Close();
    }
}
