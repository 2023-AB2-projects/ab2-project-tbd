using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace abkr.Client.GUI
{
    /// <summary>
    /// Interaction logic for EditWindow.xaml
    /// </summary>
    public partial class EditWindow : Window
    {
        private List<Dictionary<string, object>> OriginalData { get; set; }

        public EditWindow()
        {
            InitializeComponent();
        }

        public void SetData(List<Dictionary<string, object>> data)
        {
            OriginalData = data;

            // Bind the data to the DataGrid
            EditGrid.ItemsSource = OriginalData;
        }
        private void UpdateButton_Click(object sender, RoutedEventArgs e)
        {

        }

        //public void SetData(List<Dictionary<string, object>> rawData)
        //{
        //    if (rawData == null || rawData.Count == 0)
        //        return;

        //    // Create a DataTable
        //    DataTable dt = new DataTable();

        //    // Define columns
        //    foreach (var key in rawData[0].Keys)
        //    {
        //        dt.Columns.Add(key);
        //    }

        //    // Define rows
        //    foreach (var dictionary in rawData)
        //    {
        //        var row = dt.NewRow();

        //        foreach (var key in dictionary.Keys)
        //        {
        //            row[key] = dictionary[key];
        //        }

        //        dt.Rows.Add(row);
        //    }

        //    // Bind DataTable to DataGrid        
        //    EditGrid.ItemsSource = dt.DefaultView;
        //}
    }
}
