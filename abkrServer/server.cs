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

        List<string> logMessages = new List<string>(); // Store log messages


        // Initialize the DatabaseServer instance
        var databaseServer = new DatabaseServer("mongodb://localhost:27017/", "C:/Users/bfcsa/source/repos/abkr/abkrServer/Parser/example.xml");  

        bool isMetadataInSync = databaseServer.IsMetadataInSync();
        Console.WriteLine("Is metadata in sync: " + isMetadataInSync);


        while (!cts.Token.IsCancellationRequested)
        {
            // Wait for a client to connect
            TcpClient client = null; // Store the connected client

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
                LogMessage($"Error: {ex.Message}", logMessages, client, false); // Pass false for sendLogMessages
                continue;
            }
            databaseServer.ListDatabases();

            LogMessage("Client connected", logMessages, client, false); // Pass false for sendLogMessages

            // Handle the client connection and pass the DatabaseServer instance
            _ = HandleClient(client, databaseServer, cts.Token, logMessages);
        }


        // Stop the listener and close all active client connections
        listener.Stop();
        Console.WriteLine("Server stopped");
    }

    private static void LogMessage(string message, List<string> logMessages, TcpClient _connectedClient, bool sendLogMessages)
    {
        Console.WriteLine(message);
        logMessages.Add(message);

        // Send the log message to the connected client only when sendLogMessages is true
        if (_connectedClient != null && sendLogMessages && _connectedClient.Connected) // Add the _connectedClient.Connected check
        {
            SendLogMessageToClient(_connectedClient, message);
        }
    }



    private static async void SendLogMessageToClient(TcpClient client, string logMessage)
    {
        using NetworkStream stream = client.GetStream();
        using StreamWriter writer = new StreamWriter(stream, Encoding.ASCII) { AutoFlush = true };

        await writer.WriteLineAsync(logMessage);
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

    static async Task HandleClient(TcpClient client, DatabaseServer databaseServer, CancellationToken cancellationToken, List<string> logMessages) // Add logMessages as a parameter
    {
        // Get the network stream for reading and writing
        using NetworkStream stream = client.GetStream();
        using StreamReader reader = new StreamReader(stream, Encoding.ASCII);
        using StreamWriter writer = new StreamWriter(stream, Encoding.ASCII) { AutoFlush = true };

        bool sendLogMessages = false; // Add this line

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

                if (data.ToLower() == "request_log_messages")
                {
                    sendLogMessages = true;
                    continue;
                }

                LogMessage("Received: " + data, logMessages, client, sendLogMessages); // Add sendLogMessages as an argument

                string response;
                try
                {
                    await DatabaseServer.ExecuteStatementAsync(data);
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
            LogMessage($"Error: {ex.Message}", logMessages, client, sendLogMessages); // Pass sendLogMessages as an argument
        }
        //finally
        //{
        //    // Log the "Client disconnected" message before actually disconnecting the client
        //    LogMessage("Client disconnected", logMessages, client, sendLogMessages);
        //    //client.Close();
        //}

    }

}
