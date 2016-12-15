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
        public ReadOnlyCollection<DbColumn> Columns { get; private set; }
        public List<ResultRow> Rows { get; private set; }
        public int ColumnCount { get; private set; }
        public int RowCount { get; private set; }

        private Dictionary<string, int> _columnNameIndexes;


        public ResultTable(DbDataReader reader)
        {
            if (reader == null
                || !reader.CanGetColumnSchema())
            {
                throw new ArgumentException("DbDataReader has no column schema");
            }


            //Columns
            this.Columns = reader.GetColumnSchema();

            this._columnNameIndexes = new Dictionary<string, int>();
            for (var i = 0; i < this.Columns.Count; i++)
                this._columnNameIndexes.Add(this.Columns[i].ColumnName, i);

            this.ColumnCount = this.Columns.Count;

            //Rows
            this.Rows = new List<ResultRow>();
            while (reader.Read())
                this.Rows.Add(new ResultRow(this, reader));

            this.RowCount = this.Rows.Count;
        }


        public DbColumn Column(int index)
        {
            return this.Columns[index];
        }


        public DbColumn Column(string columnName)
        {
            return this.Columns[this.GetColumnIndex(columnName)];
        }


        public int GetColumnIndex(string columnName)
        {
            return this._columnNameIndexes[columnName];
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
            this.Columns = null;
            this._columnNameIndexes = null;
        }
    }
}
