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
    /// Interaction logic for SelectWindow.xaml
    /// </summary>
    public partial class SelectWindow : Window
    {
        public SelectWindow()
        {
            InitializeComponent();
        }

        public void SetData(List<Dictionary<string, object>> rawData)
        {
            if (rawData == null || rawData.Count == 0)
                return;

            // Create a DataTable
            DataTable dt = new DataTable();

            // Define columns
            foreach (var key in rawData[0].Keys)
            {
                dt.Columns.Add(key);
            }

            // Define rows
            foreach (var dictionary in rawData)
            {
                var row = dt.NewRow();

                foreach (var key in dictionary.Keys)
                {
                    row[key] = dictionary[key];
                }

                dt.Rows.Add(row);
            }

            // Bind DataTable to DataGrid
            SelectGrid.ItemsSource = dt.DefaultView;
        }
    }
}
