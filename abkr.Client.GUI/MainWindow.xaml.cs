using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using abkr.ClientLogger;

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
        private static Logger clientLogger = new Logger("C:/Users/bfcsa/github-classroom/2023-AB2-projects/ab2-project-tbd/abkr.Client.GUI/client_logger.log");
        private static Logger serverLogger = new Logger("C:/Users/bfcsa/github-classroom/2023-AB2-projects/ab2-project-tbd/abkrServer/server_logger.log");


        public MainWindow()
        {
            InitializeComponent();

            _readerSemaphore = new SemaphoreSlim(1, 1); // Initialize semaphore
            _writerSemaphore = new SemaphoreSlim(1, 1); // Initialize semaphore

            ConnectToServerAsync().ConfigureAwait(false);
        }

        private async Task ConnectToServerAsync()
        {
            _client = new TcpClient();
            await _client.ConnectAsync(IPAddress.Parse("127.0.0.1"), 1234);

            _stream = _client.GetStream();
            _reader = new StreamReader(_stream, Encoding.ASCII);
            _writer = new StreamWriter(_stream, Encoding.ASCII) { AutoFlush = true };

            string logMessage = "Connected to server. Enter SQL statements or type 'exit' to quit:";
            clientLogger.LogMessage(logMessage);
            UpdateConsole(logMessage); // Update the console
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

            string logMessage = $"Sent: {sqlStatement}";
            clientLogger.LogMessage(logMessage);
            UpdateConsole(logMessage); // Update the console
            txtInput.Clear();
        }

        private void UpdateConsole(string message)
        {
            txtConsole.AppendText(message + Environment.NewLine);
            txtConsole.ScrollToEnd();
        }


        private void Disconnect()
        {
            _client.Close();
            clientLogger.LogMessage("Disconnected from server.");
            btnSend.IsEnabled = false;
        }
    }
}
