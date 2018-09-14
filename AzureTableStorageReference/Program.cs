using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Serilog;
using System;

namespace AzureTableStorageReference
{
    class Program
    {
        // TODO: Enter your connection string here.
        private const string StorageConnectionString = "";
        private const string TableName = "testtable";

        private static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .WriteTo.Console()
                .CreateLogger();

            try
            {
                Log.Information("StorageConnectionString: {StorageConnectionString}", StorageConnectionString);
                Log.Information("TableName: {TableName}", TableName);

                Log.Information("Writing to table...");
                WriteThingToTable();

                Log.Information("Reading and deleting from table...");
                ReadAndDeleteFromTable();
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "An error has occurred.");
            }
        }

        private static void WriteThingToTable()
        {
            Log.Information("Parsing storage connection string...");
            if (!CloudStorageAccount.TryParse(StorageConnectionString, out CloudStorageAccount storageAccount))
            {
                throw new Exception("Unable to parse the storage connection string.");
            }

            Log.Information("Getting table reference...");
            var client = storageAccount.CreateCloudTableClient();
            var table = client.GetTableReference(TableName);

            Log.Information("Inserting into table...");
            table.Execute(TableOperation.Insert(new Thing("round", 10, "red")));
            table.Execute(TableOperation.Insert(new Thing("round", 20, "orange")));
            table.Execute(TableOperation.Insert(new Thing("square", 30, "yellow")));
            table.Execute(TableOperation.Insert(new Thing("angular", 40, "blue")));
            table.Execute(TableOperation.Insert(new Thing("angular", 50, "green")));
        }

        private static void ReadAndDeleteFromTable()
        {
            Log.Information("Parsing storage connection string...");
            if (!CloudStorageAccount.TryParse(StorageConnectionString, out CloudStorageAccount storageAccount))
            {
                throw new Exception("Unable to parse the storage connection string.");
            }

            Log.Information("Getting table reference...");
            var client = storageAccount.CreateCloudTableClient();
            var table = client.GetTableReference(TableName);

            Log.Information("Reading and deleting...");
            var query = new TableQuery<Thing>() { TakeCount = 1000 };
            var results = table.ExecuteQuery(query);
            foreach (var thing in results)
            {
                Log.Information("Thing {Thing}", thing);
                table.Execute(TableOperation.Delete(thing));
            }
        }
    }

    class Thing : TableEntity
    {
        public string Shape { get; set; }
        public int Size { get; set; }
        public string Color { get; set; }

        public Thing() { }

        public Thing(string shape, int size, string color)
        {
            if (string.IsNullOrWhiteSpace(shape)) throw new ArgumentNullException(nameof(shape));
            if (string.IsNullOrWhiteSpace(color)) throw new ArgumentNullException(nameof(color));

            Shape = shape;
            Size = size;
            Color = color;

            PartitionKey = shape;
            RowKey = Guid.NewGuid().ToString();
        }

        public override string ToString()
        {
            return $"({Shape}, {Size}, {Color})";
        }
    }
}
