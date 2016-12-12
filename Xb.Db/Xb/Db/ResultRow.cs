using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Xb.Db
{
    public class ResultRow : IDisposable
    {
        private object[] _items;
        private ResultTable _table;

        public ResultRow(ResultTable table, IDataRecord dataRecord)
        {
            this._table = table;

            this._items = new object[this._table.ColumnCount];
            dataRecord.GetValues(this._items);
        }


        public object Item(int index)
        {
            return this._items[index];
        }


        public object Item(string columnName)
        {
            return this._items[this._table.GetColumnIndex(columnName)];
        }


        public void Dispose()
        {
            if (this._items != null)
            {
                for (var i = 0; i < this._items.Length; i++)
                    this._items[i] = null;
            }

            this._items = null;
            this._table = null;
        }
    }
}
