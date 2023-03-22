using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

class Server {
    public static void Main() {
        // Create a TCP listener on port 1234
        TcpListener listener = new TcpListener(IPAddress.Any, 1234);
        listener.Start();

        Console.WriteLine("Server started");

        while (true) {
            // Wait for a client to connect
            TcpClient client = listener.AcceptTcpClient();
            Console.WriteLine("Client connected");

            // Handle the client connection
            HandleClient(client);
        }
    }

    static void HandleClient(TcpClient client) {
        // Get the network stream for reading and writing
        NetworkStream stream = client.GetStream();

        // Read data from the client
        byte[] buffer = new byte[1024];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);
        string data = Encoding.ASCII.GetString(buffer, 0, bytesRead);
        Console.WriteLine("Received: " + data);

        // Write a response to the client
        string response = "Hello, client!";
        byte[] responseData = Encoding.ASCII.GetBytes(response);
        stream.Write(responseData, 0, responseData.Length);

        // Close the client connection
        client.Close();
        Console.WriteLine("Client disconnected");
    }
}
