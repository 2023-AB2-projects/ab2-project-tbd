using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using abkr.ClientLogger;
using Microsoft.Win32;
using Newtonsoft.Json;

namespace abkr.Client.GUI
{
    public class DatabaseData
    {
        public string? Name { get; set; }
        public ObservableCollection<string> Tables { get; set; } = new ObservableCollection<string>();
    }

    public partial class MainWindow : Window
    {
        private TcpClient? _client;
        private NetworkStream? _stream;
        private StreamReader? _reader;
        private StreamWriter? _writer;
        private SemaphoreSlim _readerSemaphore; // Add semaphore
        private SemaphoreSlim _writerSemaphore; // Add semaphore
        private static Logger clientLogger = new Logger("C:/Users/bfcsa/github-classroom/2023-AB2-projects/ab2-project-tbd/abkr.client.GUI/client_logger.log");
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

            await ObjectExplorerAsync();
        }

        private void OpenFileButton_Click(object sender, RoutedEventArgs e)
        {
            // Open a file dialog
            var openFileDialog = new OpenFileDialog();
            openFileDialog.Filter = "SQL files (*.sql)|*.sql|All files (*.*)|*.*";
            if (openFileDialog.ShowDialog() == true)
            {
                // Load the file into the query editor
                QueryEditor.Text = File.ReadAllText(openFileDialog.FileName);
            }
        }
        private async Task ObjectExplorerAsync()
        {
            await _writer.WriteLineAsync("list-structure");

            StringBuilder structureResponseBuilder = new StringBuilder();
            string structureLine;
            while ((structureLine = await _reader.ReadLineAsync()) != null)
            {
                if (structureLine == "end")  // Stop when an empty line is encountered
                {
                    break;
                }
                structureResponseBuilder.AppendLine(structureLine);
            }

            string structureResponse = structureResponseBuilder.ToString();

            // Deserialize the response
            var structure = JsonConvert.DeserializeObject<List<DatabaseData>>(structureResponse);

            // Update the Object Explorer
            ObjExplorer.ItemsSource = structure;
        }

        private void SaveFileButton_Click(object sender, RoutedEventArgs e)
        {
            // Open a save file dialog
            var saveFileDialog = new SaveFileDialog();
            saveFileDialog.Filter = "SQL files (*.sql)|*.sql|All files (*.*)|*.*";
            if (saveFileDialog.ShowDialog() == true)
            {
                // Save the content of the query editor to the file
                File.WriteAllText(saveFileDialog.FileName, QueryEditor.Text);
            }
        }

        private void ObjExpButton_Click(object sender, RoutedEventArgs e)
        {
            if (ObjExplorer.Visibility == Visibility.Visible)
            {
                ObjExplorer.Visibility = Visibility.Collapsed;
            }
            else
            {
                ObjExplorer.Visibility = Visibility.Visible;
            }
        }
     
        private async void ExecuteButton_Click(object sender, RoutedEventArgs e)
        {
            string fullSqlStatements = QueryEditor.Text;

            var sqlStatements = new List<string>();
            var currentStatement = new StringBuilder();
            bool isInText = false;

            foreach (char c in fullSqlStatements)
            {
                if (c == ';' && !isInText)
                {
                    sqlStatements.Add(currentStatement.ToString().Trim());
                    currentStatement.Clear();
                }
                else
                {
                    if (c == '\'' || c == '"')
                    {
                        isInText = !isInText;
                    }
                    currentStatement.Append(c);
                }
            }

            // Add the last SQL statement if there's any
            var lastStatement = currentStatement.ToString().Trim();
            if (!string.IsNullOrWhiteSpace(lastStatement))
            {
                sqlStatements.Add(lastStatement);
            }

            foreach (string sqlStatement in sqlStatements)
            {
                // Send SQL statement to the server
                await _writer.WriteLineAsync(sqlStatement);

                // Receive response from server
                StringBuilder responseBuilder = new StringBuilder();
                string line;
                while ((line = await _reader.ReadLineAsync()) != null)
                {
                    if (line == "end")  // Stop when an empty line is encountered
                    {
                        break;
                    }
                    responseBuilder.AppendLine(line);
                }

                string response = responseBuilder.ToString();

                // Log the response
                UpdateConsole(response);

                // If the statement was a SELECT statement, display the result in a new window
                if (sqlStatement.Trim().ToLower().StartsWith("select"))
                {
                    var rawData = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(response);

                    var selectWindow = new SelectWindow();
                    selectWindow.SetData(rawData);
                    selectWindow.Show();
                }
            }

            await ObjectExplorerAsync();

        }

        private void OpenEditWindow(List<Dictionary<string, object>> data)
        {
            var editWindow = new EditWindow();
            editWindow.SetData(data);
            editWindow.Show();
        }

        private void UpdateConsole(string message)
        {
            LogPanel.AppendText(message + Environment.NewLine);
            LogPanel.ScrollToEnd();
        }


        private async void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            // Send "exit" to the server
            await _writer.WriteLineAsync("exit");

            // Close the connection
            _client.Close();
        }
    }
}
