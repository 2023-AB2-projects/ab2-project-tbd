using System.Net;
using System.Net.Sockets;
using System.Text;
using abkr.CatalogManager;


class Server
{
    public static void Main()
    {
        // Create a TCP listener on port 1234
        TcpListener listener = new TcpListener(IPAddress.Any, 1234);
        listener.Start();

        Console.WriteLine("Server started");

        // Initialize the DatabaseServer instance
        var databaseServer = new DatabaseServer("mongodb://localhost:27017/", "example.json");

        bool isMetadataInSync = databaseServer.IsMetadataInSync();
        Console.WriteLine("Is metadata in sync: " + isMetadataInSync);

        if (isMetadataInSync )
            databaseServer.CreateDatabase("myNewDatabase");



        while (true)
        {
            // Wait for a client to connect
            TcpClient client = listener.AcceptTcpClient();
            Console.WriteLine("Client connected");

            // Handle the client connection and pass the DatabaseServer instance
            HandleClient(client, databaseServer);
        }
    }


    static async Task HandleClient(TcpClient client, DatabaseServer databaseServer)
    {
        // Get the network stream for reading and writing
        using NetworkStream stream = client.GetStream();
        using StreamReader reader = new StreamReader(stream, Encoding.ASCII);
        using StreamWriter writer = new StreamWriter(stream, Encoding.ASCII) { AutoFlush = true };

        try
        {
            // Read data from the client
            string data = await reader.ReadLineAsync();
            Console.WriteLine("Received: " + data);

            string response;
            try
            {
                databaseServer.ExecuteStatement(data);
                response = "Success";
            }
            catch (Exception ex)
            {
                response = $"Error: {ex.Message}";
            }

            // Write a response to the client
            await writer.WriteLineAsync(response);
        }
        catch (IOException ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
        finally
        {
            client.Close();
            Console.WriteLine("Client disconnected");
        }
    }

}