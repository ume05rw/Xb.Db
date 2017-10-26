using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Xb.Db
{
    public class ResultTable : IDisposable
    {
        public string[] ColumnNames { get; private set; }
        public List<ResultRow> Rows { get; private set; }
        public int ColumnCount { get; private set; }
        public int RowCount { get; private set; }

        private Dictionary<string, int> _columnNameIndexes;


        public ResultTable(DbDataReader reader)
        {
            if (reader == null
                || reader.IsClosed)
            {
                throw new ArgumentException("Xb.Db.ResultTable: reader null or closed.");
            }

            var colNames = new List<string>();
            this._columnNameIndexes = new Dictionary<string, int>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                colNames.Add(name);
                this._columnNameIndexes.Add(name, i);
            }
            
            this.ColumnNames = colNames.ToArray();
            
            this.ColumnCount = this.ColumnNames.Length;

            //Rows
            this.Rows = new List<ResultRow>();
            while (reader.Read())
                this.Rows.Add(new ResultRow(this, reader));

            this.RowCount = this.Rows.Count;
        }

        
        public int GetColumnIndex(string columnName)
        {
            return this._columnNameIndexes[columnName];
        }

        public Dictionary<string, object>[] GetSerializable()
        {
            var result = new Dictionary<string, object>[this.RowCount];
            for (var i = 0; i < this.RowCount; i++)
            {
                result[i] = new Dictionary<string, object>();
                for (var j = 0; j < this.ColumnCount; j++)
                    result[i].Add(this.ColumnNames[j], this.Rows[i][j]);
            }

            return result;
        }


        public ResultRow NewRow()
        {
            return new ResultRow(this);
        }

        public void Dispose()
        {
            foreach (var row in this.Rows)
                row.Dispose();

            this.Rows = null;
            this._columnNameIndexes = null;
            this.ColumnNames = null;
        }
    }
}
