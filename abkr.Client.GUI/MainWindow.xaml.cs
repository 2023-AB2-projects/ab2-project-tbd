﻿using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
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

        public MainWindow()
        {
            InitializeComponent();

            ConnectToServerAsync().ConfigureAwait(false);
        }

        private async Task ConnectToServerAsync()
        {
            _client = new TcpClient();
            await _client.ConnectAsync(IPAddress.Parse("127.0.0.1"), 1234);

            _stream = _client.GetStream();
            _reader = new StreamReader(_stream, Encoding.ASCII);
            _writer = new StreamWriter(_stream, Encoding.ASCII) { AutoFlush = true };

            LogMessage("Connected to server. Enter SQL statements or type 'exit' to quit:");
        }

        private void LogMessage(string message)
        {
            txtOutput.AppendText(message + Environment.NewLine);
            txtOutput.ScrollToEnd();
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
            await _writer.WriteLineAsync(sqlStatement);

            // Receive response from server
            string response = await _reader.ReadLineAsync();
            LogMessage(response);
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
