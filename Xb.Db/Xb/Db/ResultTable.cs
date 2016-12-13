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
        private ReadOnlyCollection<DbColumn> _columns;
        public ReadOnlyCollection<DbColumn> Columns => _columns;

        private ResultRow[] _rows;
        public ResultRow[] Rows => _rows;

        private int _columnCount;
        public int ColumnCount => _columnCount;

        private int _rowCount;
        public int RowCount => _rowCount;

        private Dictionary<string, int> _columnNameIndexes;


        public ResultTable(DbDataReader reader)
        {
            if (reader == null
                || !reader.CanGetColumnSchema())
            {
                throw new ArgumentException("DbDataReader has no column schema");
            }


            //Columns
            this._columns = reader.GetColumnSchema();

            this._columnNameIndexes = new Dictionary<string, int>();
            for (var i = 0; i < this._columns.Count; i++)
                this._columnNameIndexes.Add(this._columns[i].ColumnName, i);

            this._columnCount = this._columns.Count;


            //Rows
            var rows = new List<ResultRow>();
            while (reader.Read())
                rows.Add(new ResultRow(this, reader));

            this._rows = rows.ToArray();
            this._rowCount = this._rows.Length;
        }


        public DbColumn Column(int index)
        {
            return this._columns[index];
        }


        public DbColumn Column(string columnName)
        {
            return this._columns[this.GetColumnIndex(columnName)];
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
            foreach (var row in this._rows)
                row.Dispose();

            this._rows = null;

            this._columns = null;
            this._columnNameIndexes = null;
        }
    }
}
