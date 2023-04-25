using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace abkr.ClientLogger
{ 
    public class Logger
    {
        private string _logFilePath;

        public Logger(string logFilePath)
        {
            _logFilePath = logFilePath;
        }

        public void Clear()
        {
            File.WriteAllText(_logFilePath, string.Empty);
        }

        public void LogMessage(string message)
        {
            Console.WriteLine(message);
            File.AppendAllText(_logFilePath, message + Environment.NewLine);
        }

        public List<string> ReadLogMessages()
        {
            return File.ReadAllLines(_logFilePath).ToList();
        }
    }
}