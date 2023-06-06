using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using abkr.CatalogManager;
using abkr.ServerLogger;

class Server
{
    private static CancellationTokenSource cts = new CancellationTokenSource();
    private static Logger logger = new Logger("C:/Users/bfcsa/github-classroom/2023-AB2-projects/ab2-project-tbd/abkrServer/server_logger.log");

    public static async Task Main()
    {
        // Handle the CTRL+C signal
        Console.CancelKeyPress += (sender, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cts.Cancel();
            logger.LogMessage("CTRL+C pressed. Shutting down...");
        };

        // Create a TCP listener on port 1234
        TcpListener listener = new TcpListener(IPAddress.Any, 1234);
        listener.Start();

        List<string> logMessages = new List<string>(); // Store log messages


        // Initialize the DatabaseServer instance
        var databaseServer = new DatabaseServer("mongodb://localhost:27017/", "C:/Users/bfcsa/github-classroom/2023-AB2-projects/ab2-project-tbd/abkrServer/Parser/example.xml", logger);  

        while (!cts.Token.IsCancellationRequested)
        {
            // Wait for a client to connect
            TcpClient? client = null; // Store the connected client

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
                logger.LogMessage($"Error: {ex.Message}");
                continue;
            }
            databaseServer.ListDatabases();

            logger.LogMessage("Client connected"); 
            // Handle the client connection and pass the DatabaseServer instance
            _ = HandleClient(client, databaseServer, cts.Token, logMessages);
        }


        // Stop the listener and close all active client connections
        listener.Stop();
        logger.LogMessage("Server stopped");
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

    static async Task HandleClient(TcpClient client, DatabaseServer databaseServer, CancellationToken cancellationToken, List<string> logMessages)
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
                string? data = await reader.ReadLineAsync();

                if (data == null || data.ToLower() == "exit")
                {
                    break;
                }

                logger.LogMessage("Received: " + data);

                string response;
                try
                {
                    await DatabaseServer.ExecuteStatementAsync(data);
                    if (data.Trim().ToLower().StartsWith("select"))
                    {
                        response = "Success\n" + DatabaseServer.LastQueryResult;
                    }
                    else
                    {
                        response = "Success";
                    }
                }
                catch (Exception ex)
                {
                    response = $"Error: {ex.Message}";
                }
                // Write a response to the client
                logger.LogMessage($"Server is sending: {response}");
                await writer.WriteLineAsync(response);
                await writer.WriteLineAsync("end");  // Send an empty line
            }
        }
        catch (IOException ex)
        {
            logger.LogMessage($"Error: {ex.Message}");
        }
    }
}
