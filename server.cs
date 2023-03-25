using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using MiniSQL.Library; // Import for the Parser class
using MiniSQL.Library.Models; // Import for IStatement, CreateStatement, and DropStatement
using MongoDB.Driver; // Import for MongoClient


class Server
{
    public static void Main()
    {
        // Create a TCP listener on port 1234
        TcpListener listener = new TcpListener(IPAddress.Any, 1234);
        listener.Start();

        Console.WriteLine("Server started");

        // Initialize the DatabaseServer instance
        var databaseServer = new DatabaseServer("mongodb://localhost:27017");
        Console.WriteLine("Database server initialized");

        while (true)
        {
            // Wait for a client to connect
            TcpClient client = listener.AcceptTcpClient();
            Console.WriteLine("Client connected");

            // Handle the client connection and pass the DatabaseServer instance
            HandleClient(client, databaseServer);
        }
    }

    static void HandleClient(TcpClient client)
    {
        // Get the network stream for reading and writing
        NetworkStream stream = client.GetStream();

        // Read data from the client
        byte[] buffer = new byte[1024];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);
        string data = Encoding.ASCII.GetString(buffer, 0, bytesRead);
        Console.WriteLine("Received: " + data);

        // Parse the SQL statement
        Parser parser = new Parser(data);
        IStatement statement;
        string response;
        try
        {
            statement = parser.Parse();
            // Execute the statement using the DatabaseServer instance
            databaseServer.ExecuteStatement(statement);
            response = "Success";
        }
        catch (Exception ex)
        {
            response = $"Error: {ex.Message}";
        }

        // Write a response to the client
        byte[] responseData = Encoding.ASCII.GetBytes(response);
        stream.Write(responseData, 0, responseData.Length);

        client.Close();
        Console.WriteLine("Client disconnected");
    }
}