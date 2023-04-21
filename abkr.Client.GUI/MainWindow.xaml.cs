using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;

namespace abkr.Client.GUI
{
    public partial class MainWindow : Window
    {
        private TcpClient _client;
        private NetworkStream _stream;
        private StreamReader _reader;
        private StreamWriter _writer;
        private SemaphoreSlim _readerSemaphore; // Add semaphore
        private SemaphoreSlim _writerSemaphore; // Add semaphore

        public MainWindow()
        {
            InitializeComponent();

            _readerSemaphore = new SemaphoreSlim(1, 1); // Initialize semaphore
            _writerSemaphore = new SemaphoreSlim(1, 1); // Initialize semaphore

            ConnectToServerAsync().ConfigureAwait(false);
            //RequestLogMessagesAsync().ConfigureAwait(false);
        }

        private async Task ConnectToServerAsync()
        {
            _client = new TcpClient();
            await _client.ConnectAsync(IPAddress.Parse("127.0.0.1"), 1234);

            _stream = _client.GetStream();
            _reader = new StreamReader(_stream, Encoding.ASCII);
            _writer = new StreamWriter(_stream, Encoding.ASCII) { AutoFlush = true };

            // Start receiving log messages from the server
            _ = Task.Run(async () => await ReceiveLogMessagesAsync());

            LogMessage("Connected to server. Enter SQL statements or type 'exit' to quit:");

            // Call RequestLogMessagesAsync after initializing the variables
            await RequestLogMessagesAsync();
        }


        private async Task ReceiveLogMessagesAsync()
        {
            while (_client.Connected)
            {
                try
                {
                    await _readerSemaphore.WaitAsync(); // Acquire semaphore before reading
                    string message = await _reader.ReadLineAsync();
                    _readerSemaphore.Release(); // Release semaphore after reading

                    if (message != null)
                    {
                        LogMessage(message);
                    }
                }
                catch (IOException)
                {
                    _readerSemaphore.Release(); // Release semaphore in case of an exception
                    break;
                }
            }
        }

        private void LogMessage(string message)
        {
            listBoxLog.Items.Add(message);
            listBoxLog.ScrollIntoView(listBoxLog.Items[listBoxLog.Items.Count - 1]);
        }

        private async Task RequestLogMessagesAsync()
        {
            if (_client.Connected)
            {
                await _writer.WriteLineAsync("request_log_messages");
            }
        }



        private async void btnSend_Click(object sender, RoutedEventArgs e)
        {
            string sqlStatement = txtInput.Text.Trim();

            if (string.IsNullOrWhiteSpace(sqlStatement))
            {
                return;
            }

            if (sqlStatement.ToLower() == "exit")
            {
                Disconnect();
                return;
            }

            // Send SQL statement to server
            await _writerSemaphore.WaitAsync(); // Acquire semaphore before writing
            await _writer.WriteLineAsync(sqlStatement);
            _writerSemaphore.Release(); // Release semaphore after writing

            LogMessage($"Sent: {sqlStatement}");
            txtInput.Clear();
        }



        private void Disconnect()
        {
            _client.Close();
            LogMessage("Disconnected from server.");
            btnSend.IsEnabled = false;
        }
    }
}
