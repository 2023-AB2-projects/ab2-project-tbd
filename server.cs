using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using abkr.CatalogManager;

class Server
{
    private static CancellationTokenSource cts = new CancellationTokenSource();

    public static async Task Main()
    {
        // Handle the CTRL+C signal
        Console.CancelKeyPress += (sender, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cts.Cancel();
            Console.WriteLine("Shutting down...");
        };

        // Create a TCP listener on port 1234
        TcpListener listener = new TcpListener(IPAddress.Any, 1234);
        listener.Start();

        Console.WriteLine("Server started");

        // Initialize the DatabaseServer instance
        var databaseServer = new DatabaseServer("mongodb://localhost:27017/", "C:/Users/bfcsa/source/repos/abkr/abkrServer/Parser/example.xml");  

        bool isMetadataInSync = databaseServer.IsMetadataInSync();
        Console.WriteLine("Is metadata in sync: " + isMetadataInSync);

        while (!cts.Token.IsCancellationRequested)
        {
            // Wait for a client to connect
            TcpClient client = null;
            databaseServer.ListDatabases();
            try
            {
                client = await AcceptTcpClientAsync(listener, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                continue;
            }
            databaseServer.ListDatabases();

            Console.WriteLine("Client connected");

            // Handle the client connection and pass the DatabaseServer instance
            _ = HandleClient(client, databaseServer, cts.Token);
        }

        // Stop the listener and close all active client connections
        listener.Stop();
        Console.WriteLine("Server stopped");
    }

    static async Task<TcpClient> AcceptTcpClientAsync(TcpListener listener, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<TcpClient>();

        using (cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken)))
        {
            try
            {
                tcs.TrySetResult(await listener.AcceptTcpClientAsync());
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        }

        return await tcs.Task;
    }

    static async Task HandleClient(TcpClient client, DatabaseServer databaseServer, CancellationToken cancellationToken)
    {
        // Get the network stream for reading and writing
        using NetworkStream stream = client.GetStream();
        using StreamReader reader = new StreamReader(stream, Encoding.ASCII);
        using StreamWriter writer = new StreamWriter(stream, Encoding.ASCII) { AutoFlush = true };

        try
        {
            while (!cancellationToken.IsCancellationRequested && client.Connected)
            {
                // Read data from the client
                string data = await reader.ReadLineAsync();

                if (data == null || data.ToLower() == "exit")
                {
                    break;
                }

                Console.WriteLine("Received: " + data);

                string response;
                try
                {
                    await databaseServer.ExecuteStatementAsync(data);
                    response = "Success";
                }
                catch (Exception ex)
                {
                    response = $"Error: {ex.Message}";
                }

                // Write a response to the client
                await writer.WriteLineAsync(response);
            }
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
