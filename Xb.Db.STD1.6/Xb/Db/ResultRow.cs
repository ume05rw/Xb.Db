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

        /// <summary>
        /// 構造参照先ResultTable
        /// </summary>
        public ResultTable Table => _table;

        /// <summary>
        /// 列要素インデクサ
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public object this[int index]
        {
            get { return this._items[index]; }
            set { this._items[index] = value; }
        }

        /// <summary>
        /// 列要素インデクサ
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public object this[string columnName]
        {
            get { return this._items[this._table.GetColumnIndex(columnName)];  }
            set { this._items[this._table.GetColumnIndex(columnName)] = value; }
        }


        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="table"></param>
        /// <param name="dataRecord"></param>
        public ResultRow(ResultTable table, IDataRecord dataRecord = null)
        {
            this._table = table;
            this._items = new object[this._table.ColumnCount];

            dataRecord?.GetValues(this._items);
        }


        /// <summary>
        /// 指定の型で値を取得する。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public T Get<T>(string columnName)
        {
            return (T)Convert.ChangeType(this[columnName], typeof(T));
        }


        /// <summary>
        /// 指定の型で値を取得する。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="index"></param>
        /// <returns></returns>
        public T Get<T>(int index)
        {
            return (T)Convert.ChangeType(this[index], typeof(T));
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
