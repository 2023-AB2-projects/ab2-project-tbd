using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using Newtonsoft.Json;

namespace abkr.Client.GUI
{
    public class RowEdit
    {
        public Dictionary<string, object> OriginalRow { get; set; }
        public Dictionary<string, object> EditedRow { get; set; }
    }
    public partial class EditWindow : Window
    {
        //private List<Dictionary<string, object>> OriginalData { get; set; }
        private string _databaseName;
        private string _tableName;
        private MainWindow _mainWindow;
        private DataTable _originalDataTable;
        public EditWindow(MainWindow mainWindow, string databaseName, string tableName)
        {
            InitializeComponent();

            _mainWindow = mainWindow;
            _databaseName = databaseName;
            _tableName = tableName;
        }

        public void SetData(List<Dictionary<string, object>> data)
        {
            var dataTable = new DataTable();

            if (data.Count > 0)
            {
                foreach (var key in data[0].Keys)
                {
                    dataTable.Columns.Add(key);
                }

                foreach (var row in data)
                {
                    dataTable.Rows.Add(row.Values.ToArray());
                }
            }
            _originalDataTable = dataTable.Copy();
            EditGrid.ItemsSource = dataTable.DefaultView;
        }
        private async void UpdateButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var changes = GetChanges();

                var updateData = new
                {
                    DatabaseName = _databaseName,
                    TableName = _tableName,
                    Changes = changes
                };

                var changesJson = JsonConvert.SerializeObject(updateData);

                // Send it back to the server
                await _mainWindow.SendDataToServerAsync(changesJson);
            }
            catch (Exception ex)
            {
                // Handle your exception here
                MessageBox.Show($"An error occurred: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private List<RowEdit> GetChanges()
        {
            var changes = new List<RowEdit>();

            var dataView = (DataView)EditGrid.ItemsSource;
            var editedDataTable = dataView.Table;

            foreach (DataRow row in editedDataTable.Rows)
            {
                var originalRow = _originalDataTable.Rows.Find(row["id"]);  // Assuming "Id" is the primary key
                if (originalRow == null) continue;

                // Convert DataRow to Dictionary
                var originalRowDict = originalRow.Table.Columns.Cast<DataColumn>()
                    .ToDictionary(col => col.ColumnName, col => originalRow[col]);

                var editedRow = row;

                // Convert DataRow to Dictionary
                var editedRowDict = editedRow.Table.Columns.Cast<DataColumn>()
                    .ToDictionary(col => col.ColumnName, col => editedRow[col]);

                if (!originalRowDict.SequenceEqual(editedRowDict))
                {
                    changes.Add(new RowEdit
                    {
                        OriginalRow = originalRowDict,
                        EditedRow = editedRowDict
                    });
                }
            }

            return changes;
        }

    }
}
